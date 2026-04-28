from uuid import uuid4
import traceback
from modules.login import decode_access_token
from datetime import date, datetime
import base64 as _b64
import base64
from pydantic import BaseModel
from datetime import datetime, time as dt_time
from fastapi import APIRouter, UploadFile, File, HTTPException, Query, status, Depends
from modules.extraction import ZipExtractionService
from modules.categorization import CategorizationService
from modules.processing import ProcessingService
from modules.llm_service import analyze_transaction
from modules.session import session_service
from modules.login import decode_access_token
from modules.transaction_analyzer import TransactionAnalyzerService
from modules.schemas import (
    FileCategorizationResponse,
    AvailableFileTypesResponse,
    FileTypeSelectionRequest,
    CategoryCount, 
    TransactionVisualizationRequest
	
)
from admin_setup import create_dn_diagnostics_database, initialize_admin_table
from modules.login import create_login_history_table, create_reset_tokens_table
from modules.analysis import (
        create_userresponse_database,
        create_analysis_table,
        create_feedback_table,
    )

from modules.extraction import extract_from_directory, extract_from_zip_bytes, resolve_main_zips
from modules.xml_parser_logic import parse_xml_to_dataframe
from pathlib import Path
from typing import Dict, List, Optional
import shutil
from fastapi import Body, Header
from pydantic import BaseModel
import os
import pandas as pd
from modules.ui_journal_processor  import UIJournalProcessor, parse_ui_journal
from modules.journal_parser import match_journal_file, mask_ej_log
from modules.ui_journal_processor  import UIJournalProcessor, parse_ui_journal, parse_ui_journal_from_string
from datetime import datetime, date
from collections import defaultdict
import re
import zipfile
import io
import json
import time



#  Import our central logger
from modules.logging_config import logger
from modules.analysis import store_metadata, store_feedback, get_user_role, get_analysis_records, get_feedback_records
from modules.login import (verify_reset_identity,generate_reset_token,send_reset_email,validate_reset_token,reset_user_password,is_valid_password,authenticate_user_backend,register_user,is_user_pending_approval,log_login_event,create_access_token)

import logging
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from modules.flat_file_generator import FlatFileMerger
from api.chunk_service import assemble_and_process, cancel_upload, save_chunk
from fastapi import APIRouter, File, Form, UploadFile
from typing import Optional

logger.info("Logger initialized at startup")

router = APIRouter()
logger.info("FastAPI app started")

# ── Counter Analysis ─────────────────────────────────────────────────────────
# Imported after require_elevated_role is defined below — see bottom of this
# block for the include_router call.
# ─────────────────────────────────────────────────────────────────────────────


# ============================================
# RBAC DEPENDENCY
# ============================================
# Roles allowed to access all endpoints.
# USER role is restricted to individual-transaction endpoints only.
ALLOWED_ROLES = {"ADMIN", "DEV_MODE"}

async def require_elevated_role(
    authorization: str = Header(default=None),
):
    """
    FastAPI dependency — reads the JWT from Authorization: Bearer header,
    decodes it, and asserts the role is ADMIN or DEV_MODE.

    Returns 401 if token is missing or invalid.
    Returns 403 if role is not in ALLOWED_ROLES (i.e. role == USER).
    Both responses are logged to the terminal.
    """

    if not authorization or not authorization.startswith("Bearer "):
        logger.warning(
            "RBAC [401] — missing Authorization header on elevated endpoint"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "status": "error",
                "code": 401,
                "error": "Unauthorized",
                "message": (
                    "Authentication token missing. "
                    "Log in through the application to access this endpoint."
                ),
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization.split(" ", 1)[1]
    payload = decode_access_token(token)   # raises 401 automatically if bad
    role = payload.get("role", "")

    if role not in ALLOWED_ROLES:
        logger.warning(
            "RBAC [403] — user='%s' role='%s' denied on elevated endpoint",
            payload.get("sub"), role,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "status": "error",
                "code": 403,
                "error": "Forbidden",
                "message": (
                    f"Access denied. Role '{role}' does not have permission "
                    f"to access this endpoint. Required role: ADMIN or DEV_MODE."
                ),
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

# ── Counter Analysis import (after require_elevated_role is defined) ──────────
# ─────────────────────────────────────────────────────────────────────────────

log_file=Path("app.log")

# ============================================
@router.get("/read-log")
async def read_log():
    """
    FUNCTION:
        read_log

    DESCRIPTION:
        Reads the main application log file and returns its content as a response.

    USAGE:
        result = await read_log()

    PARAMETERS:
        None

    RETURNS:
        dict : 
            {
                "status": "success",
                "log_content": "<full log text>"
            }
            OR
            {
                "status": "error",
                "message": "Log file does not exist"
            }

    RAISES:
        None explicitly raised.
    """
    if os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8") as f:
            content = f.read()
            logger.info("Log file read successfully")
        return {"status": "success", "log_content": content}
    else:
        logger.error("Log file does not exist")
        return {"status": "error", "message": "Log file does not exist"}



# ============================================
# DEBUG ENDPOINT: List ZIP Members
# ============================================

@router.post("/debug-zip-members")
async def debug_zip_members(file: UploadFile = File(...)):
    """
    FUNCTION:
        debug_zip_members

    DESCRIPTION:
        Receives a ZIP file upload and returns a detailed list of all members (files and directories) 
        in the archive. Filters XML and XSD files and identifies those matching certain patterns 
        (e.g., starting with 'jdd' or 'x3'). This is mainly used for debugging ZIP structures 
        and ACU file detection.

    USAGE:
        result = await debug_zip_members(file=some_upload_file)

    PARAMETERS:
        file (UploadFile) : The ZIP file uploaded via multipart/form-data.

    RETURNS:
        dict : 
            {
                "status": "success" or "error",
                "total_members": <total number of members in ZIP>,
                "all_members": [list of dicts with member info],
                "xml_files": [list of XML files],
                "xsd_files": [list of XSD files],
                "matching_xml_jdd_x3": [XML files starting with 'jdd' or 'x3'],
                "matching_xsd_jdd_x3": [XSD files starting with 'jdd' or 'x3'],
                "note": "summary string"
            }
            OR
            {
                "status": "error",
                "error": "<error message>",
                "traceback": "<stack trace if exception>"
            }

    RAISES:
        BadZipFile : If the uploaded file is not a valid ZIP archive.
        Exception  : Any unexpected errors during reading or processing the ZIP file.
    """
    session_id = _resolve_session_id(session_id)
    logger.info(" Received request: /debug-zip-members")  
    try:
        zip_bytes = await file.read()
        logger.debug(f"Read {len(zip_bytes)} bytes from uploaded file: {file.filename}") 
        members = []
        
        # Try to open as standard ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                logger.info("zip opened successfully  :%s,file.filename")
                for info in zf.infolist():
                    member_info = {
                        "path": info.filename,
                        "basename": os.path.basename(info.filename),
                        "is_dir": info.is_dir(),
                        "compressed_size": info.compress_size,
                        "uncompressed_size": info.file_size,
                        "compress_type": info.compress_type
                    }
                    members.append(member_info)
                    logger.debug(f"ZIP member found: {member_info}") 
                    
        except zipfile.BadZipFile as e:
            logger.error(f"BadZipFile encountered for {file.filename}: {e}")  
            return {
                "status": "error",
                "error": f"BadZipFile: {str(e)}",
                "note": "Archive may be corrupted or use non-standard format. Low-level extractor will be needed.",
                "members": []
            }
        
        # Filter for potential ACU files
        xml_files = [m for m in members if m["basename"].lower().endswith('.xml')]
        xsd_files = [m for m in members if m["basename"].lower().endswith('.xsd')]
        logger.debug(f"XML files found: {len(xml_files)}, XSD files found: {len(xsd_files)}")  
        
        # Check which would match with current patterns (jdd, x3)
        matching_xml = [m for m in xml_files if m["basename"].lower().startswith(('jdd', 'x3'))]
        matching_xsd = [m for m in xsd_files if m["basename"].lower().startswith(('jdd', 'x3'))]
        logger.debug(f"Matching XML jdd/x3: {len(matching_xml)}, Matching XSD jdd/x3: {len(matching_xsd)}")
        
        return {
            "status": "success",
            "total_members": len(members),
            "all_members": members,
            "xml_files": xml_files,
            "xsd_files": xsd_files,
            "matching_xml_jdd_x3": matching_xml,
            "matching_xsd_jdd_x3": matching_xsd,
            "note": f"Matching check: {len(matching_xml)} XML files with jdd/x3 prefix, {len(matching_xsd)} XSD files with jdd/x3 prefix"
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in /debug-zip-members: {e}")  
        logger.debug(traceback.format_exc()) 
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# Simple session ID for now (use UUID in production)
global CURRENT_SESSION_ID
CURRENT_SESSION_ID = str(uuid4())

# ── Session-ID resolver ───────────────────────────────────────────────────────
_SESSION_SENTINELS = {"current_session", "CURRENT_SESSION_ID", "", None}

def _resolve_session_id(session_id) -> str:
    """
    Resolve a client-supplied session_id to the real UUID.

    Accepts:
        - None / empty string  → returns CURRENT_SESSION_ID
        - "current_session"    → returns CURRENT_SESSION_ID  (frontend sentinel)
        - any other string     → returned as-is
    """
    if session_id in _SESSION_SENTINELS:
        return CURRENT_SESSION_ID
    return session_id
# ──────────────────────────────────────────────────────────────────────────────

from modules.counter_analysis import init_counter_router, counter_router
init_counter_router(require_elevated_role, _resolve_session_id)
router.include_router(counter_router)

# Global variable to track processed files directory (for registry endpoints)
PROCESSED_FILES_DIR = None

def set_processed_files_dir(directory: str):
    """Set the directory where processed files are stored"""
    global PROCESSED_FILES_DIR
    PROCESSED_FILES_DIR = directory
    logger.info(f"Processed files directory set to: {directory}")  

def get_processed_files_dir() -> str:
    return PROCESSED_FILES_DIR


@router.post("/process-zip", response_model=FileCategorizationResponse)
async def process_zip_file(file: UploadFile = File(..., description="ZIP file to process"),mode: Optional[str] = Query(None, description="Processing mode (e.g., 'registry' to optimize for registry files)")):
    """
    FUNCTION:
        process_zip_file

    DESCRIPTION:
        Receives a ZIP file upload, extracts its contents (including nested ZIPs),
        categorizes files into predefined categories (e.g., customer journals, registry files, ACU files),
        extracts ACU XML/XSD files, updates the session with processed data, 
        and prepares a response summarizing the categorized files.

    USAGE:
        result = await process_zip_file(file=some_zip_file, mode="registry")

    PARAMETERS:
        file (UploadFile) : The ZIP file uploaded via multipart/form-data.
        mode (str, optional): Optional processing mode to influence categorization.

    RETURNS:
        FileCategorizationResponse : 
            Object containing categorized files, ACU extraction logs, and other metadata.

    RAISES:
        HTTPException : 
            - 400 if uploaded file is not a ZIP.
            - 500 if extraction, categorization, or processing fails.
        Exception     : Any unexpected errors during processing.
    """
    """
    Step 1: Receive and validate ZIP file upload
    """
    logger.info(" Received request to process ZIP file")
    if not file.filename.lower().endswith('.zip'):
        logger.error(" Invalid file type - only ZIP allowed")
        raise HTTPException(
            status_code=400,
            detail="Only ZIP files are accepted"
        )
    logger.info(f" Uploaded file name: {file.filename}")
    
    try:
        

        # -----------------------------------------------------------
        # ⏱️ START TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        start_time = time.perf_counter()
        # -----------------------------------------------------------

        ZipExtractionService().cleanup_old_extracts(max_age_hours=0.5)

        # ------------------ FILE READ TIMER ------------------
        t_file_start = time.perf_counter()
        logger.debug("Reading uploaded file ")

        zip_content = await file.read()  # read only once
        t_file_end = time.perf_counter()
        logger.info(f" File read completed. Size: {len(zip_content)} bytes")

        logger.debug(f"FILE READ TIME: {t_file_end - t_file_start:.4f} s")

        # ------------------ ZIP EXTRACTION TIMER ------------------
        logger.info("Extracting ZIP ...")
        t_zip_start = time.perf_counter()

        try:
            extraction_service = ZipExtractionService()
            extract_path, total_files_in_zip, acu_zip_bytes_list = extraction_service.extract_zip(zip_content)
            all_files_on_disk = [p for p in Path(extract_path).rglob('*') if p.is_file()]
            total_files_on_disk = len(all_files_on_disk)
            logger.info(f"Total files in original ZIP: {total_files_in_zip}")
            logger.info(f"Total files in extracted directory: {total_files_on_disk}")
        except TypeError as te:
            logger.error(f"TypeError calling extract_zip: {te}")
            raise
        except Exception as ex:
            logger.info(f"Extraction failed: {ex}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"ZIP extraction failed: {ex}")

        t_zip_end = time.perf_counter()
        logger.info(f"ZIP EXTRACTION TIME: {t_zip_end - t_zip_start:.4f} s")
        logger.debug(f"Extracted directory: {extract_path}")

        # ------------------ NESTED ZIP EXTRACTION ------------------
		# now this works directly from the ZipExtractionService.extract_zip(). function so that all the files are stored in their respective folder in Temp -> dn_extracts
        logger.info("STEP 3: Nested ZIP files from ZipExtractionService.")
        t_nested_zip_start = time.perf_counter()

        nested_zip_files = [p for p in Path(extract_path).rglob('*.zip')]
        logger.info(f"Nested ZIPs present in EXTRA branch (kept for reference): {len(nested_zip_files)}")
        t_nested_zip_end = time.perf_counter()
        logger.info(f"NESTED ZIP EXTRACTION TIME: {t_nested_zip_end - t_nested_zip_start:.4f} s")

        # ------------------ CATEGORIZATION + ACU EXTRACTION (COMBINED) ------------------
        # Extract ACU files first, then include in categorization result
        logger.info(f" Extracting ACU XML files (jdd*, x3*) directly from ZIP...")
        acu_logs = []
        t_cat_start = time.perf_counter()
        
        #  Extract ACU files directly from the already-decompressed acu.zip bytes that
        #  extract_zip() captured and returned. This avoids re-opening the main ZIP and
        #  re-decompressing acu.zip a second time.
        try:
            acu_files = {}
            if acu_zip_bytes_list:
                for acu_zip_bytes in acu_zip_bytes_list:
                    partial = extract_from_zip_bytes(acu_zip_bytes, acu_logs, target_prefixes=('jdd', 'x3'))
                    for key, content in partial.items():
                        # Use the same _1, _2 suffix dedup as the on-disk ACU/ branch so
                        # that duplicate filenames from multiple acu.zip copies are all kept.
                        dedup_key = key
                        i = 1
                        while dedup_key in acu_files:
                            base, ext = os.path.splitext(key)
                            dedup_key = f"{base}_{i}{ext}"
                            i += 1
                        acu_files[dedup_key] = content
            else:
                logger.info(" No acu.zip found in uploaded package — skipping ACU extraction.")
            xml_count = sum(1 for k in acu_files if not k.startswith('__xsd__'))
            xsd_count = sum(1 for k in acu_files if k.startswith('__xsd__'))
            logger.info(f" ACU extraction: {xml_count} XML, {xsd_count} XSD files")

            # Get a set of base filenames for exclusion during disk scan
            acu_filenames_to_exclude = {os.path.basename(p) for p in acu_files.keys()}
        except Exception as e:
            logger.error(f" Error extracting ACU files: {str(e)}")
            
            acu_files = {}
            acu_filenames_to_exclude = set()
            acu_logs.append(f"Error: {str(e)}")
        
        # Step 2: Categorize files from the extracted directory.
        # Values are full disk paths at this stage — used only during the loading
        # phase below, then replaced by filename-only lists before session storage.
        file_categories = {
            'customer_journals': [], 'ui_journals': [], 'trc_trace': [],
            'trc_error': [], 'registry_files': [], 'acu_files': [],
            'journal_llm_files': [], 'unidentified': []
        }
        
        # Step 3: Add the correctly identified ACU files to the categories FIRST
        # if acu_files:
        #     file_categories['acu_files'] = list(acu_files.keys())
        #     logger.info(f" Added {len(acu_files)} ACU files to final categories.")
        
        # Step 4: Run on-disk categorization, which will populate the SAME dictionary
        logger.info("Running on-disk categorization ...")
        categorization_service = CategorizationService()
        categorization_service.categorize_files(extract_path, file_categories, set(), mode=mode)
        logger.info(f" ACU files from disk (ACU/ branch): {len(file_categories['acu_files'])}")

        t_cat_end = time.perf_counter()
        logger.debug(f"CATEGORIZATION + ACU EXTRACTION TIME: {t_cat_end - t_cat_start:.4f} s")

        # ── FLAT FILE MERGER ──────────────────────────────────────────────────
        # Runs after categorization while file_categories still holds full paths.
        # Normal run (buffer only, inspect via /read-log):
        FlatFileMerger.run(
            customer_paths=file_categories.get('customer_journals', []),
            ui_paths=file_categories.get('ui_journals', []),
            llm_paths=file_categories.get('journal_llm_files', []),
        )

        #to-do: add a query param to trigger writing the merged files to disk for verification, and to inspect the logs to confirm correct files were merged. This will be removed after verification is complete.
        # With physical file written to disk for verification:
        # FlatFileMerger.run(
        #     customer_paths=file_categories.get('customer_journals', []),
        #     ui_paths=file_categories.get('ui_journals', []),
        #     llm_paths=file_categories.get('journal_llm_files', []),
        #     write_to_disk=True,
        #     output_dir=Path("merged_output"),
        #  )

        

        # ------------------ IN-MEMORY FILE LOAD ------------------
        # Read every relevant branch's file contents from Temp into session memory.
        # After this block succeeds the run folder is deleted.
        #
        # Branches loaded: REGISTRY, CUSTOMER, UI, TRC (ACU already in acu_files).
        # EXTRA branch: no content is read — each file is registered as an empty dict
        # TO-DO in case of any future feature require.

        t_sess_start = time.perf_counter()

        def _read_text(p: Path) -> str:
            """Read a file trying common encodings, falling back to latin1."""
            for enc in ('utf-8', 'latin1', 'windows-1252', 'utf-16'):
                try:
                    return p.read_text(encoding=enc, errors='strict')
                except Exception:
                    continue
            return p.read_text(encoding='latin1', errors='replace')

        # --- REGISTRY ---
        # Base64-encode bytes before storing so the session remains JSON-serialisable.
        registry_contents: dict = {}
        for path_str in file_categories.get('registry_files', []):
            p = Path(path_str)
            try:
                registry_contents[p.name] = _b64.b64encode(p.read_bytes()).decode('utf-8')
                logger.debug(f"[REGISTRY] Mapped and base64-encoded content for: {p.name}")
            except Exception as e:
                logger.error(f"[REGISTRY] failed to load {p.name}: {e}")

        # --- CUSTOMER JOURNALS ---
        customer_journal_contents: dict = {}
        for path_str in file_categories.get('customer_journals', []):
            p = Path(path_str)
            try:
                customer_journal_contents[p.name] = _read_text(p)
                logger.debug(f"[CUSTOMER] Mapped content to filename: {p.name}")
            except Exception as e:
                logger.error(f"[CUSTOMER] failed to load {p.name}: {e}")

        # --- UI JOURNALS ---
        ui_journal_contents: dict = {}
        for path_str in file_categories.get('ui_journals', []):
            p = Path(path_str)
            try:
                ui_journal_contents[p.name] = _read_text(p)
                logger.debug(f"[UI] Mapped content to filename: {p.name}")
            except Exception as e:
                logger.error(f"[UI] failed to load {p.name}: {e}")

        # --- JOURNAL-FOLDER WITH FILES FOR LLM---
        # These come from the JOURNAL folder (not from VCP-PRO/JOURNALS/UI) and must NOT be passed to the UI Flow of Individual Transaction feature.
        journal_llm_contents: dict = {}
        for path_str in file_categories.get('journal_llm_files', []):
            p = Path(path_str)
            try:
                journal_llm_contents[p.name] = _read_text(p)
                logger.debug(f"[JOURNAL] Mapped content to filename: {p.name}")
            except Exception as e:
                logger.error(f"[JOURNAL] failed to load {p.name}: {e}")

        # --- TRC TRACE ---
        trc_trace_contents: dict = {}
        for path_str in file_categories.get('trc_trace', []):
            p = Path(path_str)
            try:
                trc_trace_contents[p.name] = _read_text(p)
                logger.debug(f"[TRC_TRACE] Mapped content to filename: {p.name}")
            except Exception as e:
                logger.error(f"[TRC_TRACE] failed to load {p.name}: {e}")

        # --- TRC ERROR ---
        trc_error_contents: dict = {}
        for path_str in file_categories.get('trc_error', []):
            p = Path(path_str)
            try:
                trc_error_contents[p.name] = _read_text(p)
                logger.debug(f"[TRC_ERROR] Mapped content to filename {p.name}")
            except Exception as e:
                logger.error(f"[TRC_ERROR] failed to load {p.name}: {e}")

        # --- EXTRA ---
        # No content is read. Each file in the EXTRA branch is registered as an empty dict placeholder keyed by filename. 
        extra_contents: dict = {}
        for path_str in file_categories.get('unidentified', []):
            p = Path(path_str)
            extra_contents[p.name] = {}
            logger.debug(f"[EXTRA] File loaded into empty object: {p.name}")

        logger.info(
            f"In-memory load complete:"
            f"registry:{len(registry_contents)} "
            f"customer:{len(customer_journal_contents)} "
            f"ui:{len(ui_journal_contents)} "
            f"trc_trace:{len(trc_trace_contents)} "
            f"trc_error:{len(trc_error_contents)} "
            f"extra:{len(extra_contents)}"
        )

        # Convert file_categories from full disk paths to filenames only.
        for branch in ('customer_journals', 'ui_journals', 'trc_trace', 'trc_error',
                        'registry_files', 'acu_files', 'journal_llm_files', 'unidentified'):
            file_categories[branch] = [Path(p).name for p in file_categories[branch]]

        
        # ------------------ SESSION CREATION------------------
        logger.info("Creating/updating session")

        set_processed_files_dir(None)
        session_service.create_session(CURRENT_SESSION_ID, file_categories, None)

        # Confirm new session after creation
        new_sess = session_service.get_session(CURRENT_SESSION_ID)
        
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extracted_files',acu_files)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extraction_logs',acu_logs)
        session_service.update_session(CURRENT_SESSION_ID, 'registry_contents',registry_contents)
        session_service.update_session(CURRENT_SESSION_ID, 'customer_journal_contents',customer_journal_contents)
        session_service.update_session(CURRENT_SESSION_ID, 'ui_journal_contents',ui_journal_contents)
        session_service.update_session(CURRENT_SESSION_ID, 'journal_llm_contents',journal_llm_contents)
        session_service.update_session(CURRENT_SESSION_ID, 'trc_trace_contents',trc_trace_contents)
        session_service.update_session(CURRENT_SESSION_ID, 'trc_error_contents',trc_error_contents)
        session_service.update_session(CURRENT_SESSION_ID, 'extra_contents', extra_contents)

        t_sess_end = time.perf_counter()
        logger.debug(f"SESSION SAVE TIME: {t_sess_end - t_sess_start:.4f} s")

        # Delete the run folder from Temp so all content is now in memory.
        try:
            shutil.rmtree(extract_path, ignore_errors=False)
            logger.info(f"Temp run folder deleted: {extract_path}")
        except Exception as e:
            logger.error(f"Failed to delete Temp run folder {extract_path}: {e}")


        # ------------------ PROCESSING TIMER ------------------
        logger.info(" prepare response")
        t_proc_start = time.perf_counter()
        processing_service = ProcessingService()
        result = processing_service.prepare_response(file_categories, extract_path)
        result.acu_extraction_logs = acu_logs
        t_proc_end = time.perf_counter()
        logger.debug(f"PROCESSING TIME: {t_proc_end - t_proc_start:.4f} s")


        # -----------------------------------------------------------
        #  END TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        end_time = time.perf_counter()
        total_time = round(end_time - start_time, 2)
        logger.info(f"TOTAL ZIP PROCESSING TIME: {total_time:.4f} s")
        # -----------------------------------------------------------

        # Attach processing time to response dict so frontend can display it
        result_dict = result.dict() if hasattr(result, "dict") else dict(result)
        result_dict["processing_time_seconds"] = total_time
        result_dict["session_id"] = CURRENT_SESSION_ID   
        return result_dict
    except HTTPException:
        raise   
    except Exception as e:
        logger.error(f" ERROR in process_zip:{e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error processing ZIP file: {str(e)}"
        )



@router.post("/extract-files/")
async def extract_files_from_zip(file: UploadFile = File(...)):
    """
    FUNCTION:
        extract_files_from_zip

    DESCRIPTION:
        Receives a ZIP file upload, extracts ACU XML/XSD files with prefixes 'jdd' or 'x3'
        for comparison or analysis purposes, and returns the extracted files along with logs.

    USAGE:
        result = await extract_files_from_zip(file=some_zip_file)

    PARAMETERS:
        file (UploadFile) : The ZIP file uploaded via multipart/form-data.

    RETURNS:
        dict :
            {
                "files": dict,   # Keys are filenames, values are file contents
                "logs": list,    # Extraction logs and messages
                "message": str   # Summary of extraction result
            }

    RAISES:
        HTTPException :
            - 400 if uploaded file is not a ZIP
            - 500 if extraction fails due to unexpected errors
        Exception : Any other unexpected errors during extraction
    """
    """
    Extract ACU files from an uploaded ZIP for comparison purposes.
    """
    logger.info(f" Received request: /extract-files/ for file: {file.filename}")  
    try:
        if not file.filename.lower().endswith('.zip'):
            logger.error(f"Invalid file type uploaded: {file.filename}")  
            raise HTTPException(
                status_code=400,
                detail="Only ZIP files are accepted"
            )

        zip_content = await file.read()
        logger.debug(f"Read {len(zip_content)} bytes from uploaded file: {file.filename}")

        acu_logs = []

        # --- Resolve shell ZIP if needed, then drill into acu.zip ---
        # resolve_main_zips handles the case where the user uploaded a shell ZIP
        # that wraps the real main ZIP(s). For a normal upload it is a no-op fast path.
        # We then search each confirmed main ZIP for acu.zip and extract from it.
        acu_files: dict = {}
        try:
            main_zip_list = resolve_main_zips(zip_content)
            for main_zip_bytes in main_zip_list:
                with zipfile.ZipFile(io.BytesIO(main_zip_bytes)) as outer_zf:
                    acu_candidates = [
                        name for name in outer_zf.namelist()
                        if os.path.basename(name).lower() == 'acu.zip'
                    ]
                    if acu_candidates:
                        for candidate in acu_candidates:
                            candidate_bytes = outer_zf.read(candidate)
                            logger.info(f"Processing nested acu.zip at: {candidate}")
                            partial = extract_from_zip_bytes(candidate_bytes, acu_logs, target_prefixes=('jdd', 'x3'))
                            for key, content in partial.items():
                                dedup_key = key
                                i = 1
                                while dedup_key in acu_files:
                                    base, ext = os.path.splitext(key)
                                    dedup_key = f"{base}_{i}{ext}"
                                    i += 1
                                acu_files[dedup_key] = content
                    else:
                        logger.info("No nested acu.zip found — treating main ZIP as the ACU archive directly")
                        partial = extract_from_zip_bytes(main_zip_bytes, acu_logs, target_prefixes=('jdd', 'x3'))
                        for key, content in partial.items():
                            dedup_key = key
                            i = 1
                            while dedup_key in acu_files:
                                base, ext = os.path.splitext(key)
                                dedup_key = f"{base}_{i}{ext}"
                                i += 1
                            acu_files[dedup_key] = content
        except zipfile.BadZipFile as e:
            logger.error(f"BadZipFile when scanning for nested acu.zip in {file.filename}: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid ZIP file: {e}")
        logger.debug(f"ACU extraction logs: {acu_logs}")

        if not acu_files:
            logger.info(f"No ACU files found in uploaded ZIP: {file.filename}")  
            return {
                "files": {},
                "logs": acu_logs,
                "message": "No ACU files found in the uploaded ZIP"
            }

        logger.info(f"Successfully extracted {len(acu_files)} ACU file(s) from: {file.filename}")  
        logger.debug(f"Extracted ACU files: {list(acu_files.keys())}") 

        return {
            "files": acu_files,
            "logs": acu_logs,
            "message": f"Successfully extracted {len(acu_files)} file(s)"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during ACU extraction for file {file.filename}: {e}")  
        logger.debug(traceback.format_exc())  
        raise HTTPException(
            status_code=500,
            detail=f"Error extracting files: {str(e)}\n{traceback.format_exc()}"
        )

@router.post("/extract-registry-from-zip")
async def extract_registry_from_zip(file: UploadFile = File(...), session_id: str = Query(default=None)):
    """
    FUNCTION:
        extract_registry_from_zip

    DESCRIPTION:
        Extracts ONLY registry-related files from an uploaded ZIP archive.

        Uses the same ZipExtractionService + CategorizationService pipeline as
        /process-zip so that both Package A (main upload) and Package B (compare
        upload) identify registry files with identical logic.  

    PARAMETERS:
        file (UploadFile) : ZIP file uploaded via multipart/form-data.

    RETURNS:
        dict :
            {
                "registry_contents" : dict   # filename -> base64-encoded bytes
                "count"             : int    # number of registry files found
                "message"           : str    # human-readable summary
            }

    RAISES:
        HTTPException :
            - 400 if the uploaded file is not a ZIP
            - 500 for unexpected errors during extraction
    """
    session_id = _resolve_session_id(session_id)
    logger.info(f"Received request: /extract-registry-from-zip for file: {file.filename}")

    if not file.filename.lower().endswith('.zip'):
        raise HTTPException(status_code=400, detail="Only ZIP files are accepted")

    try:
        

        zip_bytes = await file.read()
        logger.info(f"Read {len(zip_bytes)} bytes from {file.filename}")

        # ------------------------------------------------------------------
        # Step 1: Extract ZIP using the same ZipExtractionService used by
        # /process-zip.  This routes every file into branch folders (REGISTRY/,
        # ACU/, TRC/, …) using _classify_to_branch() — identical logic to what
        # builds Package A's registry_contents in the session.
        # ------------------------------------------------------------------
        extraction_service = ZipExtractionService()
        try:
            extract_path, _, _ = extraction_service.extract_zip(zip_bytes)
        except Exception as e:
            logger.error(f"ZIP extraction failed: {e}")
            raise HTTPException(status_code=400, detail=f"ZIP extraction failed: {str(e)}")

        # ------------------------------------------------------------------
        # Step 2: Read every file that landed in the REGISTRY/ branch folder.
        # CategorizationService is not needed here — anything in REGISTRY/ is
        # already a registry file by construction.
        # ------------------------------------------------------------------
        registry_branch = extract_path / "REGISTRY"
        registry_contents: dict = {}

        def _dedup_key(basename: str, existing: dict) -> str:
            key = basename
            counter = 1
            while key in existing:
                name_part, ext_part = os.path.splitext(basename)
                key = f"{name_part}_{counter}{ext_part}"
                counter += 1
            return key

        if registry_branch.exists():
            for reg_file in registry_branch.iterdir():
                if not reg_file.is_file():
                    continue
                try:
                    key = _dedup_key(reg_file.name, registry_contents)
                    registry_contents[key] = _b64.b64encode(reg_file.read_bytes()).decode('utf-8')
                    logger.info(f"Loaded registry file: {reg_file.name} -> key={key}")
                except Exception as e:
                    logger.warning(f"Could not read registry file {reg_file.name}: {e}")
        else:
            logger.warning(f"REGISTRY branch folder not found at {registry_branch}")

        # ------------------------------------------------------------------
        # Step 3: Clean up the temp extraction folder
        # ------------------------------------------------------------------
        try:
            shutil.rmtree(extract_path, ignore_errors=True)
            logger.info(f"Cleaned up temp folder: {extract_path}")
        except Exception as e:
            logger.warning(f"Could not clean up temp folder {extract_path}: {e}")

        count = len(registry_contents)
        logger.info(f"Registry extraction complete: {count} file(s) found in {file.filename}")

        if count == 0:
            return {
                "registry_contents": {},
                "count": 0,
                "message": "No registry files found in the uploaded ZIP"
            }

        return {
            "registry_contents": registry_contents,
            "count": count,
            "message": f"Successfully extracted {count} registry file(s)"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in /extract-registry-from-zip: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error extracting registry files: {str(e)}"
        )

@router.get("/get-registry-contents", dependencies=[Depends(require_elevated_role)])
async def get_registry_contents(session_id: Optional[str] = Query(default=None)):
    """
FUNCTION: get_registry_contents

DESCRIPTION:
    Retrieves registry file contents from a given session. 
    Converts binary registry file contents to base64 strings for JSON serialization 
    and returns them along with a count of available files.

USAGE:
    response = await get_registry_contents(session_id="session_123")

PARAMETERS:
    session_id (str) : Optional. The ID of the session to retrieve registry files from.
                       Defaults to CURRENT_SESSION_ID if not provided.

RETURNS:
    dict : A dictionary containing:
        - "registry_contents" (dict) : Mapping of file names to base64-encoded file contents
        - "count" (int)               : Number of registry files available in the session

RAISES:
    HTTPException : 
        - 404 : If the session with the given ID does not exist
        - 500 : For any other unexpected server error during retrieval
"""
    # Resolve values ("current_session", None, "") → real UUID
    session_id = _resolve_session_id(session_id)

    try:
        if not session_service.session_exists(session_id):
            raise HTTPException(status_code=404, detail="No session found")
        
        session_data = session_service.get_session(session_id)

        
        # Convert bytes to base64 for JSON serialization
        raw_contents = session_data.get('registry_contents', {})
        encoded_contents = {}
        for filename, content in raw_contents.items():
            if isinstance(content, bytes):
                encoded_contents[filename] = base64.b64encode(content).decode('utf-8')
            else:
                encoded_contents[filename] = content   # already a base64 string

        logger.info(f"Serving {len(encoded_contents)} registry files from session '{session_id}'")

        return {
            "registry_contents": encoded_contents,
            "count": len(encoded_contents)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving registry contents: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# ACU PARSER ENDPOINTS
# ============================================

@router.get("/get-acu-files", dependencies=[Depends(require_elevated_role)])
async def get_acu_files(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    FUNCTION:
        get_acu_files

    DESCRIPTION:
        Retrieves the list of ACU XML files (excluding XSD) and extraction logs
        from the current session. Used by frontend to display available ACU files
        for parsing or further processing.

    USAGE:
        result = await get_acu_files(session_id="current_session")

    PARAMETERS:
        session_id (str) : Optional session ID to fetch ACU files from. 
                            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                "acu_files": list,  # List of ACU XML filenames
                "logs": list,       # Extraction logs for reference/debug
                "count": int,       # Number of ACU XML files found
                "message": str      # Summary message
            }

    RAISES:
        HTTPException :
            - 404 if session does not exist
            - 500 if unexpected errors occur during retrieval
    """
    session_id = _resolve_session_id(session_id)
    try:
        if not session_service.session_exists(session_id):
            logger.warning(f"No session found for session_id: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload a ZIP file first."
            )
        
        # Get ACU files from session
        session = session_service.get_session(session_id)
        acu_files = session.get('acu_extracted_files', {})
        acu_logs = session.get('acu_extraction_logs', [])
        logger.info(f"SESSION KEYS FOR DEBUG: {list(session.keys())}")

        logger.info(f" type acu_files {type(acu_files)}, initial 5 elements {list(acu_files.keys())[:5]}")
        logger.info(f" SP type acu_logs {type(acu_logs)}, initial 5 elements {acu_logs[:5]}")


        logger.debug(f"Retrieved {len(acu_files)} ACU files and {len(acu_logs)} extraction logs from session {session_id}")
        
        if not acu_files or not isinstance(acu_files, dict):
            logger.info(f"No ACU files found in session {session_id}")
            return {
                "acu_files": [],
                "logs": acu_logs or [],
                "message": "No ACU files found in the processed package"
            }
        
        # Filter out XSD files (they start with __xsd__)
       
        xml_files = [f for f in acu_files if not f.startswith('__xsd__')]

        logger.info(f"Found {len(xml_files)} ACU XML file(s) in session {session_id}")

        # NEW FIX — return filename → content dict
        xml_dict = {fname: acu_files[fname] for fname in xml_files}

        return {
            "acu_files": xml_dict,       # -- What frontend expects!
            "logs": acu_logs or [],
            "count": len(xml_dict),
            "message": f"Found {len(xml_dict)} ACU XML file(s)"
        }

        """xml_files = [f for f in acu_files if not f.startswith('__xsd__')]

        logger.info(f">> SP list {xml_files[:5]}")
        logger.info(f"Found {len(xml_files)} ACU XML file(s) in session {session_id}")
        
        
        return {
            "acu_files": xml_files,
            "logs": acu_logs or [],
            "count": len(xml_files),
            "message": f"Found {len(xml_files)} ACU XML file(s)"
        }"""
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrieving ACU files for session {session_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving ACU files: {str(e)}"
        )

@router.post("/parse-acu-files", dependencies=[Depends(require_elevated_role)])
async def parse_acu_files(files_to_parse: List[dict] = Body(...),session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    FUNCTION:
        parse_acu_files

    DESCRIPTION:
        Parses selected ACU XML files from a session using the advanced ACU parser.
        It looks up the XML content (and corresponding XSD if available) in the session,
        converts them into structured records, and returns parsing results with logs.

    USAGE:
        result = await parse_acu_files(
            files_to_parse=[{"filename": "jdd_config.xml"}],
            session_id="current_session"
        )

    PARAMETERS:
        files_to_parse (list of dicts) :
            List of dictionaries containing filenames to parse.
            Example: [{"filename": "jdd_config.xml"}]
        session_id (str) :
            Session ID where the extracted ACU files are stored.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                "data": list,          # Parsed records from XML files
                "logs": list,          # Logs describing parsing actions, successes, and failures
                "total_records": int   # Total number of records parsed from all files
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 400 if no ACU files are found in the session
            - 500 if any unexpected error occurs during parsing
    """
    session_id = _resolve_session_id(session_id)
    try:
        if not session_service.session_exists(session_id):
            logger.warning(f"No session found for session_id: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload a ZIP file first."
            )
        
        # Get all extracted ACU files from session
        acu_files = session_service.get_session_data(session_id, 'acu_extracted_files')
        logger.debug(f"Retrieved ACU files from session {session_id}")
        
        if not acu_files:
            logger.info(f"No ACU files found in session {session_id}")
            raise HTTPException(
                status_code=400,
                detail="No ACU files found in the processed package."
            )
        
        # Create a lookup dict for easy access
        file_lookup = {f: content for f, content in acu_files.items() if isinstance(acu_files, dict)}
        
        all_parsed_data = []
        logs = []
        
        for file_info in files_to_parse:
            filename = file_info.get('filename')
            
            if not filename:
                logs.append("Skipped: No filename provided")
                logger.debug("Skipped parsing because filename was not provided")
                continue
            
            logger.debug(f"Processing file: {filename}")
            
            # Look up the file content
            xml_content = None
            if isinstance(acu_files, dict):
                xml_content = acu_files.get(filename)
            else:
                # acu_files might be a list, search for matching file
                for f in acu_files:
                    if isinstance(f, str) and f.endswith(filename):
                        try:
                            with open(f, 'r', encoding='utf-8', errors='replace') as file:
                                xml_content = file.read()
                            break
                        except Exception as e:
                            logs.append(f"Failed to read {filename}: {e}")
                            logger.error(f"Failed to read file {filename}: {e}")
                            continue
            
            if not xml_content:
                logs.append(f"File not found in extracted package: {filename}")
                logger.warning(f"File not found in extracted package: {filename}")
                continue
            
            # Look for matching XSD
            xsd_content = None
            xml_basename = os.path.splitext(os.path.basename(filename))[0].lower()
            xsd_key = f'__xsd__{xml_basename}'
            
            if isinstance(acu_files, dict) and xsd_key in acu_files:
                xsd_content = acu_files[xsd_key]
            
            try:
                # Parse using the consolidated parser
                df = parse_xml_to_dataframe(
                    xml_content=xml_content,
                    filename=filename,
                    xsd_content=xsd_content
                )
                
                if df is not None and not df.empty:
                    all_parsed_data.extend(df.to_dict('records'))
                    logs.append(f" Parsed {filename}: {len(df)} records")
                    logger.info(f"Parsed {filename}: {len(df)} records")
                else:
                    logs.append(f" No data extracted from {filename}")
                    logger.warning(f"No data extracted from {filename}")
                    
            except Exception as e:
                logs.append(f" Failed to parse {filename}: {str(e)}")
                logger.error(f"Failed to parse {filename}: {str(e)}")
        
        logger.info(f"Parsing complete: {len(all_parsed_data)} total records")
        return {
            "data": all_parsed_data,
            "logs": logs,
            "total_records": len(all_parsed_data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error parsing ACU files: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error parsing ACU files: {str(e)}"
        )



@router.get("/available-file-types", response_model=AvailableFileTypesResponse)
async def get_available_file_types(session_id: str = Query(default=None)):
    """
    FUNCTION:
        get_available_file_types

    DESCRIPTION:
        Retrieves the list of available file categories/types from a processed ZIP session.
        It checks the session for categorized files and returns only non-empty categories
        along with their file counts and filenames.

    USAGE:
        result = await get_available_file_types(session_id="current_session")

    PARAMETERS:
        session_id (str) :
            Session ID where the ZIP file was processed.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        AvailableFileTypesResponse :
            {
                "available_types": list of str,   # List of non-empty file categories
                "type_details": dict               # Details per category with file count and filenames
                    {
                        "<category_name>": {
                            "count": int,       # Number of files in this category
                            "files": list[str]  # List of filenames
                        },
                        ...
                    }
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 404 if no file categories are found in the session
    """
    session_id = _resolve_session_id(session_id)
    logger.info(f" Received request: /available-file-types for session_id: {session_id}") 

    # Check if session exists
    if not session_service.session_exists(session_id):
        logger.error(f"No processed ZIP found for session_id: {session_id}") 
        raise HTTPException(
            status_code=404,
            detail="No processed ZIP found. Please upload a ZIP file first."
        )

    # Get file categories
    file_categories = session_service.get_file_categories(session_id)
    logger.debug(f"Retrieved file categories for session_id {session_id}: {list(file_categories.keys()) if file_categories else 'None'}")  # DEBUG log

    if not file_categories:
        logger.error(f"No file categories found for session_id: {session_id}")  
        raise HTTPException(
            status_code=404,
            detail="No file categories found"
        )

    # Filter only non-empty categories
    available_types = []
    type_details = {}

    for category, files in file_categories.items():
        if len(files) > 0:
            available_types.append(category)
            type_details[category] = CategoryCount(
                count=len(files),
                files=[Path(f).name for f in files]
            )
            logger.debug(f"Category '{category}' has {len(files)} file(s)")  

    logger.info(f"Available file types for session_id {session_id}: {available_types}")  

    return AvailableFileTypesResponse(
        available_types=available_types,
        type_details=type_details
    )

@router.post("/select-file-type")
async def select_file_type(request: FileTypeSelectionRequest,session_id: str = Query(default=None)):
    """
    FUNCTION:
        get_available_file_types

    DESCRIPTION:
        Retrieves the list of available file categories/types from a processed ZIP session.
        It checks the session for categorized files and returns only non-empty categories
        along with their file counts and filenames.

    USAGE:
        result = await get_available_file_types(session_id="current_session")

    PARAMETERS:
        session_id (str) :
            Session ID where the ZIP file was processed.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        AvailableFileTypesResponse :
            {
                "available_types": list of str,   # List of non-empty file categories
                "type_details": dict               # Details per category with file count and filenames
                    {
                        "<category_name>": {
                            "count": int,       # Number of files in this category
                            "files": list[str]  # List of filenames
                        },
                        ...
                    }
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 404 if no file categories are found in the session
    """
    session_id = _resolve_session_id(session_id)
    logger.info(f" Received request: /select-file-type for session_id: {session_id}")  

    # Check if session exists
    if not session_service.session_exists(session_id):
        logger.error(f"No processed ZIP found for session_id: {session_id}") 
        raise HTTPException(
            status_code=404,
            detail="No processed ZIP found. Please upload a ZIP file first."
        )

    # Get file categories
    file_categories = session_service.get_file_categories(session_id)
    logger.debug(f"Retrieved file categories for session_id {session_id}: {list(file_categories.keys()) if file_categories else 'None'}")  

    if not file_categories:
        logger.error(f"No file categories found for session_id: {session_id}")  
        raise HTTPException(
            status_code=404,
            detail="No file categories found"
        )

    # Get selected file types - convert enum to string
    session_id = _resolve_session_id(session_id)
    try:
        selected_types = [ft.value if hasattr(ft, 'value') else str(ft) for ft in request.file_types]
        logger.debug(f"Selected file types for session_id {session_id}: {selected_types}")  
    except Exception as e:
        logger.error(f"Invalid file types format for session_id {session_id}: {str(e)}")  
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file types format: {str(e)}"
        )

    # Validate all selected types
    for selected_type in selected_types:
        if selected_type not in file_categories or len(file_categories[selected_type]) == 0:
            logger.error(f"No files found for type '{selected_type}' in session_id {session_id}") 
            raise HTTPException(
                status_code=400,
                detail=f"No files found for type: {selected_type}"
            )

    # Store selected types in session
    session_service.update_session(session_id, 'selected_types', selected_types)
    logger.info(f"Stored selected types in session_id {session_id}: {selected_types}")  

    # Define available operations for each file type
    operations_map = {
        "customer_journals": [
            "parse_transactions",
            "analyze_transactions",
            "generate_report",
            "root_cause_analysis",
            "transaction_flow_visualization"
        ],
        "ui_journals": [
            "parse_ui_events",
            "analyze_ui_flow"
        ],
        "trc_trace": [
            "parse_trace_logs",
            "error_detection"
        ],
        "trc_error": [
            "parse_error_logs",
            "critical_error_summary"
        ],
        "registry_files": [
            "parse_registry",
            "compare_registry"
        ]
    }

    # Collect operations from all selected types
    available_operations = []
    for selected_type in selected_types:
        if selected_type in operations_map:
            available_operations.extend(operations_map[selected_type])

    # Remove duplicates while preserving order
    available_operations = list(dict.fromkeys(available_operations))
    logger.debug(f"Available operations for session_id {session_id}: {available_operations}")  
    logger.debug(f"File counts per type for session_id {session_id}: {{ {selected_type: len(file_categories[selected_type]) for selected_type in selected_types} }}")  

    return {
        "selected_types": selected_types,
        "available_operations": available_operations,
        "file_counts": {
            selected_type: len(file_categories[selected_type])
            for selected_type in selected_types
        }
    }

@router.post("/analyze-customer-journals")
async def analyze_customer_journals(session_id: str = Query(default=None)):
    """
    FUNCTION:
        analyze_customer_journals

    DESCRIPTION:
        Analyzes customer journal files from a processed ZIP session and extracts transaction data.
        Combines transactions from all customer journal files, calculates statistics per transaction type,
        and updates the session with extracted transaction data and source file details.
        If UI journal files are present in the session, each transaction is enriched with
        JRN fields (protocol steps, device errors, response code, STAN, retract counter, etc.).

    USAGE:
        result = await analyze_customer_journals(session_id="current_session")

    PARAMETERS:
        session_id (str) :
            Session ID containing the processed ZIP files.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                'message': str,                   # Status message
                'total_transactions': int,        # Total transactions extracted
                'statistics': list[dict],         # Transaction stats per type
                'source_files': list[str],        # List of journal source filenames
                'source_file_count': int,         # Number of source files processed
                'jrn_enriched': int               # Transactions enriched with UI journal data (0 if no JRN files)
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 400 if no customer journal files are found
            - 400 if no transactions could be extracted
            - 500 for unexpected errors during processing
    """
    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f" Starting customer journal analysis for session: {session_id}")
        
        # Check if session exists
        if not session_service.session_exists(session_id):
            logger.error("Session not found")
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload a ZIP file first."
            )
        
        # Get file categories from session
        file_categories = session_service.get_file_categories(session_id)
        journal_files = file_categories.get('customer_journals', [])
        journal_contents = session_service.get_session_data(session_id, 'customer_journal_contents') or {}
        ui_journal_files = file_categories.get('ui_journals', [])

        if not journal_files:
            logger.error("No customer journal files found")
            raise HTTPException(
                status_code=400,
                detail="No customer journal files found in the uploaded package."
            )
        
        logger.info(f" Found {len(journal_files)} customer journal file(s)")
        logger.info(f" Found {len(ui_journal_files)} UI journal file(s)")

        # Initialize analyzer
        analyzer = TransactionAnalyzerService()

        all_transactions_df = []
        source_files = []
        source_file_map = {}
        
        for journal_filename in journal_files:
            logger.info(f"   Processing: {journal_filename}")
            source_filename = Path(journal_filename).stem
            source_files.append(source_filename)

            content = journal_contents.get(journal_filename)
            if content is None:
                logger.error(f"Content not found in session for {journal_filename}")
                continue
            
            try:
                df = analyzer.parse_customer_journal_from_string(content, journal_filename)
                
                if df is None or df.empty:
                    logger.debug(f"No transactions found in {source_filename}")
                    continue

                logger.info(f"Found {len(df)} transactions in {source_filename}")

                all_transactions_df.append(df)

                if 'Transaction ID' in df.columns:
                    source_file_map[source_filename] = df['Transaction ID'].tolist()

            except Exception as e:
                logger.error(f"Error processing {journal_filename}: {str(e)}")
                traceback.print_exc()
                continue

        if not all_transactions_df:
            logger.error("No transactions extracted from customer journal files")
            raise HTTPException(
                status_code=400,
                detail="No transactions could be extracted from the customer journal files."
            )

        combined_df = pd.concat(all_transactions_df, ignore_index=True)

        logger.info(f"\n BEFORE RENAME:")
        logger.info(f"   Total rows: {len(combined_df)}")
        logger.info(f"   Columns: {combined_df.columns.tolist()}")
        if 'Source_File' in combined_df.columns:
            logger.info(f"   Unique Source_File values: {combined_df['Source_File'].unique()}")

        if 'Source_File' in combined_df.columns:
            combined_df = combined_df.rename(columns={'Source_File': 'Source File'})

        logger.info(f"Total transactions extracted: {len(combined_df)}")
        logger.info(f"Total source files: {len(source_files)}")

        if 'Source File' in combined_df.columns:
            unique_sources_in_data = combined_df['Source File'].unique().tolist()
            logger.debug(f"Source files in data: {unique_sources_in_data}")
            logger.debug(f"Source files list: {source_files}")

        transaction_records = combined_df.to_dict('records')

        logger.info(f"\n CONVERTING TO RECORDS:")
        logger.info(f"   Total records: {len(transaction_records)}")
        if transaction_records:
            sample = transaction_records[0]
            logger.info(f"   Sample record keys: {list(sample.keys())}")
            logger.info(f"   Sample 'Source File' value: '{sample.get('Source File', 'KEY NOT FOUND')}'")
        
        # Preserve all source file names including deduplicated variants (_1, _2, …).
        # Do NOT collapse via set() — duplicate .jrn files in the ZIP are stored in
        # the session as  20250404, 20250404_1, 20250404_2 … and each must remain a
        # distinct entry so that the Counters Analysis dropdown can surface them all.
        # We still deduplicate consecutive identical names that arise from the same
        # filename appearing twice in the loop (shouldn't happen, but guard anyway).
        seen = set()
        unique_source_files = []
        for sf in source_files:
            if sf not in seen:
                seen.add(sf)
                unique_source_files.append(sf)
        unique_source_files = sorted(unique_source_files)

        logger.info(f" Unique source files being stored: {unique_source_files}")
        logger.info(f" Total source files count: {len(unique_source_files)}")

        session_service.update_session(session_id, 'transaction_data', transaction_records)
        session_service.update_session(session_id, 'source_files', unique_source_files)
        session_service.update_session(session_id, 'source_file_map', source_file_map)

        # ── Statistics ────────────────────────────────────────────────────────
        stats = []
        for txn_type in combined_df['Transaction Type'].unique():
            type_df      = combined_df[combined_df['Transaction Type'] == txn_type]
            successful   = len(type_df[type_df['End State'] == 'Successful'])
            unsuccessful = len(type_df[type_df['End State'] == 'Unsuccessful'])
            total        = len(type_df)

            stats.append({
                'Transaction Type': txn_type,
                'Total':            total,
                'Successful':       successful,
                'Unsuccessful':     unsuccessful,
                'Success Rate':     f"{(successful / total * 100):.1f}%" if total > 0 else "0%"
            })

        # Count how many transactions were enriched with JRN data
        _jrn_cols    = [c for c in combined_df.columns if c.startswith("JRN ")]
        jrn_enriched = int(combined_df[_jrn_cols].notna().any(axis=1).sum()) \
        if _jrn_cols else 0

        logger.info(f"Customer journal analysis completed. JRN enriched: {jrn_enriched}")

        return {
            'message':              'Customer journals analyzed successfully',
            'total_transactions':   len(combined_df),
            'statistics':           stats,
            'source_files':         source_files,
            'source_file_count':    len(source_files),
            'jrn_enriched':         jrn_enriched,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )
@router.get("/get-transactions-with-sources")
async def get_transactions_with_sources(session_id: str = Query(default=None)):
    """
    FUNCTION:
        get_transactions_with_sources

    DESCRIPTION:
        Retrieves all transactions extracted from customer journal files along with source file information.
        Provides a mapping of which transaction came from which source file, and a list of all transactions.

    USAGE:
        result = await get_transactions_with_sources(session_id="current_session")

    PARAMETERS:
        session_id (str) :
            Session ID containing the processed ZIP files and analyzed transactions.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                'source_files': list[str],        # List of customer journal source filenames
                'source_file_map': dict,          # Mapping: source filename -> list of transaction IDs
                'all_transactions': list[dict],   # Full transaction records
                'total_transactions': int         # Count of all transactions
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 500 for unexpected errors while retrieving data
    """
    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f"Fetching transactions with source mapping for session: {session_id}")
        logger.debug("Entered /get-transactions-with-sources route")

        logger.info(f" Getting transactions with sources for session: {session_id}")
        
        if not session_service.session_exists(session_id):
            logger.error(f"Session not found: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload and analyze files first."
            )
        
        logger.debug(f"Session exists. Retrieving session data for: {session_id}")
        session_data = session_service.get_session(session_id)
        
        transaction_data = session_data.get('transaction_data', [])
        source_files = session_data.get('source_files', [])
        source_file_map = session_data.get('source_file_map', {})
        
        # Remove duplicates - keep only unique source files
        source_files = list(set(source_files))
        source_files.sort()
        # DEBUG: Check what's actually in the transaction data
        if transaction_data:
            actual_sources_in_data = set(txn.get('Source File', '') for txn in transaction_data)
            logger.info(f"   Source files from stored list: {source_files}")
            logger.info(f"   Actual 'Source File' values in transaction data: {actual_sources_in_data}")
            logger.info(f"   Do they match? {set(source_files) == actual_sources_in_data}")
            
        logger.debug(f"Source file map keys: {list(source_file_map.keys())}")

        logger.info(f" Found {len(transaction_data)} transactions from {len(source_files)} source files")
        
        return {
            'source_files': source_files,
            'source_file_map': source_file_map,
            'all_transactions': transaction_data,
            'total_transactions': len(transaction_data)
        }
    
    except HTTPException:
        logger.error("HTTPException triggered while fetching transaction data")
        raise
    
    except Exception as e:
        logger.error(f"Unexpected error retrieving transactions: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving transactions: {str(e)}"
        )


@router.post("/filter-transactions-by-sources")
async def filter_transactions_by_sources(source_files: List[str] = Body(..., embed=True),session_id: str = Query(default=None)):
    """
    FUNCTION:
        filter_transactions_by_sources

    DESCRIPTION:
        Filters previously extracted transactions based on the selected source files.
        Returns only the transactions that originated from the specified customer journal files.

    USAGE:
        result = await filter_transactions_by_sources(
            source_files=["CustomerJournal_1.txt", "CustomerJournal_2.txt"],
            session_id="current_session"
        )

    PARAMETERS:
        source_files (list[str]) :
            List of source file names to filter transactions by.
        session_id (str) :
            Session ID containing the analyzed transactions. Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                'transactions': list[dict],  # Transactions matching the selected source files
                'count': int,                # Number of filtered transactions
                'source_files': list[str]    # List of source files used for filtering
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 400 if transaction data is missing in the session
            - 500 for unexpected errors during filtering
    """
    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f" Filtering transactions by {len(source_files)} source file(s)")
        logger.info(f" Requested source files: {source_files}")
        
        
        if not session_service.session_exists(session_id):
            logger.error(f"Session not found: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found."
            )
        
        logger.debug(f"Session exists. Fetching session data for: {session_id}")
        session_data = session_service.get_session(session_id)
        transaction_data = session_data.get('transaction_data', [])
        
        if not transaction_data:
            logger.error("Transaction data missing. Cannot filter transactions.")
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )
        
        # DEBUG: Check what's in the data before filtering
        logger.info(f"   Total transactions before filter: {len(transaction_data)}")
        if transaction_data:
            sample_txn = transaction_data[0]
            logger.info(f"   Sample transaction 'Source File': '{sample_txn.get('Source File', 'KEY NOT FOUND')}'")
            
            # Get all unique source files in the data
            actual_sources = set(txn.get('Source File', '') for txn in transaction_data)
            logger.info(f"   Actual unique source files in data: {actual_sources}")
        
        # Filter transactions by source file
        logger.debug("Applying source file filters to transaction list.")
        filtered_transactions = [
            txn for txn in transaction_data
            if txn.get('Source File') in source_files
        ]
        
        logger.info(f"   Filtered to {len(filtered_transactions)} transactions")
        
        if len(filtered_transactions) == 0:
            logger.info(f"  WARNING: No transactions matched!")
            logger.info(f"  Requested: {source_files}")
            logger.info(f"  Available: {actual_sources}")
        
        return {
            'transactions': filtered_transactions,
            'count': len(filtered_transactions),
            'source_files': source_files
        }
    
    except HTTPException as http_err:
        logger.error(f"HTTP error while filtering transactions: {http_err.detail}")
        raise
    
    except Exception as e:
        logger.error(f"Unexpected error while filtering transactions: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error filtering transactions: {str(e)}"
        )

@router.get("/transaction-statistics", dependencies=[Depends(require_elevated_role)])
async def get_transaction_statistics(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    FUNCTION:
        get_transaction_statistics

    DESCRIPTION:
        Generates statistics from previously analyzed customer journal transactions.
        Provides counts of successful and unsuccessful transactions per transaction type
        along with the success rate.

    USAGE:
        result = await get_transaction_statistics(session_id="current_session")

    PARAMETERS:
        session_id (str) :
            Session ID containing the analyzed transactions. Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                'statistics': list[dict],       # Statistics per transaction type
                'total_transactions': int       # Total number of transactions analyzed
            }
            Each item in 'statistics' contains:
            {
                'Transaction Type': str,
                'Total': int,
                'Successful': int,
                'Unsuccessful': int,
                'Success Rate': str
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 400 if no transaction data is available in the session
            - 500 for unexpected errors during statistics generation
    """

    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f"Request received: Get transaction statistics for session {session_id}")
        if not session_service.session_exists(session_id):
            logger.error(f"Session not found: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload and analyze files first."
            )
        
        logger.debug(f"Fetching session data for session {session_id}")
        session_data = session_service.get_session(session_id)
        transaction_data = session_data.get('transaction_data')
            
        if not transaction_data:
            logger.error("No transaction data found in session.")
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )
        
        logger.debug("Converting transaction data into DataFrame for analysis")
        df = pd.DataFrame(transaction_data)
        
        logger.debug("Generating statistics by transaction type")
        stats = []
        for txn_type in df['Transaction Type'].unique():
            type_df = df[df['Transaction Type'] == txn_type]
            successful = len(type_df[type_df['End State'] == 'Successful'])
            unsuccessful = len(type_df[type_df['End State'] == 'Unsuccessful'])
            total = len(type_df)
            
            # Calculate average duration
            if 'Duration (seconds)' in type_df.columns:
                avg_duration = type_df['Duration (seconds)'].mean()
                avg_duration_str = f"{avg_duration:.1f}s" if not pd.isna(avg_duration) else "N/A"
            else:
                avg_duration_str = "N/A"
            
            stats.append({
                'Transaction Type': txn_type,
                'Count': total,
                'Successful': successful,
                'Unsuccessful': unsuccessful,
                'Success Rate': f"{(successful/total*100):.1f}%" if total > 0 else "0%",
                'Avg Duration': avg_duration_str
            })
        
        logger.info("Transaction statistics generated successfully")

        return {
            'statistics': stats,
            'total_transactions': len(transaction_data)
        }
        
    except HTTPException as http_err:
        logger.error(f"HTTP error while generating statistics: {http_err.detail}")
        raise
    
    except Exception as e:
        logger.error(f"Unexpected error while generating statistics: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error generating statistics: {str(e)}"
        )

@router.post("/compare-transactions-flow", dependencies=[Depends(require_elevated_role)])
async def compare_transactions_flow(txn1_id: str = Body(...),txn2_id: str = Body(...),session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    FUNCTION:
        compare_transactions_flow

    DESCRIPTION:
        Compares the UI flows of two customer transactions within a session.
        Extracts screens from UI journal files corresponding to each transaction,
        calculates matching screens using Longest Common Subsequence (LCS),
        and provides detailed analysis including duration, common and unique screens,
        and source file information.

    USAGE:
        result = await compare_transactions_flow(
            txn1_id="TXN12345",
            txn2_id="TXN67890",
            session_id="current_session"
        )

    PARAMETERS:
        txn1_id (str):
            ID of the first transaction to compare.
        txn2_id (str):
            ID of the second transaction to compare.
        session_id (str):
            Session ID containing extracted transactions and UI journals.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                "txn1_id": str,
                "txn2_id": str,
                "txn1_type": str,
                "txn2_type": str,
                "txn1_state": str,
                "txn2_state": str,
                "txn1_flow": list[str],        # Screens for txn1
                "txn2_flow": list[str],        # Screens for txn2
                "txn1_matches": list[bool],    # LCS match flags for txn1 screens
                "txn2_matches": list[bool],    # LCS match flags for txn2 screens
                "txn1_log": str,               # Original transaction log
                "txn2_log": str,
                "detailed_analysis": str       # Human-readable analysis summary
            }

    RAISES:
        HTTPException :
            - 404 if session or transactions are not found
            - 400 if transaction data is missing
            - 500 for unexpected errors during comparison

    NOTES:
        - Uses UI journal files to reconstruct transaction screen flows.
        - Computes LCS to identify common screens between transactions.
        - Provides timing/duration analysis if start and end times are available.
        - Handles missing or empty UI journals gracefully.
    """
    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f" Comparing transactions: {txn1_id} vs {txn2_id}")

        # Check session
        if not session_service.session_exists(session_id):
            logger.error(f"No session found for session_id: {session_id}")
            raise HTTPException(status_code=404, detail="No session found")

        session_data = session_service.get_session(session_id)

        # Get transaction data
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            logger.warning("No transaction data available.")
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )

        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)

        # Check if both transactions exist
        txn1_exists = len(df[df['Transaction ID'] == txn1_id]) > 0
        txn2_exists = len(df[df['Transaction ID'] == txn2_id]) > 0

        if not txn1_exists:
            logger.error(f"Transaction {txn1_id} not found")
            raise HTTPException(status_code=404, detail=f"Transaction {txn1_id} not found")

        if not txn2_exists:
            logger.error(f"Transaction {txn2_id} not found")
            raise HTTPException(status_code=404, detail=f"Transaction {txn2_id} not found")

        # Get transaction details
        txn1_data = df[df['Transaction ID'] == txn1_id].iloc[0]
        txn2_data = df[df['Transaction ID'] == txn2_id].iloc[0]

        logger.info(f" Found both transactions: {txn1_id}, {txn2_id}")

        # Get file categories and UI journal contents from session
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        ui_journal_contents = session_data.get('ui_journal_contents', {})

        logger.info(f" Found {len(ui_journals)} UI journal file(s)")

        # Extract UI flows for both transactions
        ui_flow_1 = ["No screens in time range"]
        ui_flow_2 = ["No screens in time range"]

        if ui_journals:
            try:
                txn1_source_file = str(txn1_data.get('Source File', ''))
                txn2_source_file = str(txn2_data.get('Source File', ''))

                logger.debug(f"Transaction 1 source: {txn1_source_file}")
                logger.debug(f"Transaction 2 source: {txn2_source_file}")

                def extract_flow_with_durations(txn_data, txn_source_file, txn_label):
                    flow_screens = ["No screens in time range"]

                    # Use journal_parser.match_journal_file for exact stem matching
                    matching_ui_journal = match_journal_file(txn_source_file, ui_journals)
                    ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals

                    for ui_journal_filename in ui_journals_to_check:
                        logger.info(f" Parsing UI journal for {txn_label}: {ui_journal_filename}")

                        content = ui_journal_contents.get(ui_journal_filename, '')
                        ui_df = parse_ui_journal_from_string(content, ui_journal_filename)

                        if not ui_df.empty:
                            logger.info(f" Parsed {len(ui_df)} UI events for {txn_label}")

                            processor = UIJournalProcessor.__new__(UIJournalProcessor)
                            processor.file_path = Path(ui_journal_filename)
                            processor.df = ui_df

                            def parse_time_local(time_str):
                                if pd.isna(time_str):
                                    return None
                                if isinstance(time_str, str):
                                    try:
                                        return datetime.strptime(time_str, '%H:%M:%S').time()
                                    except:
                                        return None
                                elif hasattr(time_str, 'time'):
                                    return time_str.time()
                                return time_str

                            # Extract flow
                            start_time = parse_time_local(txn_data['Start Time'])
                            end_time = parse_time_local(txn_data['End Time'])
                            
                            if start_time and end_time:
                                logger.info(f" {txn_label} time range: {start_time} to {end_time}")
                                unique_screens = processor.get_screen_flow(start_time, end_time)
                                
                                if unique_screens and len(unique_screens) > 0:
                                    # Now add durations
                                    try:
                                        # Auto-detect columns
                                        time_col = None
                                        screen_col = None
                                        
                                        for col in ['Time', 'time', 'timestamp', 'Timestamp', 'TimeStamp']:
                                            if col in ui_df.columns:
                                                time_col = col
                                                break
                                        
                                        for col in ['ScreenName', 'Screen', 'screen', 'screen_name']:
                                            if col in ui_df.columns:
                                                screen_col = col
                                                break
                                        
                                        if time_col and screen_col:
                                            # Ensure time column is in time format
                                            if ui_df[time_col].dtype == 'object' or str(ui_df[time_col].dtype).startswith('datetime'):
                                                ui_df[time_col] = pd.to_datetime(ui_df[time_col], errors='coerce').dt.time
                                            
                                            # Filter events in time range
                                            ui_filtered = ui_df[
                                                (ui_df[time_col] >= start_time) & 
                                                (ui_df[time_col] <= end_time)
                                            ].copy()
                                            
                                            if len(ui_filtered) > 0:
                                                # Build map of screens to times
                                                screen_info = {}
                                                for screen_name in unique_screens:
                                                    occurrences = []
                                                    for idx, row in ui_filtered.iterrows():
                                                        screen = str(row.get(screen_col, ''))
                                                        time_val = row.get(time_col)
                                                        if screen == screen_name and time_val:
                                                            occurrences.append(time_val)
                                                    
                                                    if occurrences:
                                                        screen_info[screen_name] = {
                                                            'first_time': occurrences[0]
                                                        }
                                                
                                                # Build detailed flow
                                                flow_details = []
                                                for i, screen_name in enumerate(unique_screens):
                                                    info = screen_info.get(screen_name)
                                                    
                                                    if not info:
                                                        flow_details.append({
                                                            'screen': screen_name,
                                                            'timestamp': '',
                                                            'duration': None
                                                        })
                                                        continue
                                                    
                                                    first_time = info['first_time']
                                                    
                                                    # Calculate duration to next screen
                                                    duration = None
                                                    if i < len(unique_screens) - 1:
                                                        next_screen = unique_screens[i + 1]
                                                        next_info = screen_info.get(next_screen)
                                                        
                                                        if next_info and next_info['first_time']:
                                                            try:
                                                                dt1 = datetime.combine(date.today(), first_time)
                                                                dt2 = datetime.combine(date.today(), next_info['first_time'])
                                                                duration = (dt2 - dt1).total_seconds()
                                                            except:
                                                                duration = None
                                                    
                                                    flow_details.append({
                                                        'screen': screen_name,
                                                        'timestamp': str(first_time),
                                                        'duration': duration
                                                    })
                                                
                                                if flow_details:
                                                    flow_screens = flow_details
                                                    logger.info(f" {txn_label} flow with durations: {len(flow_details)} screens")
                                                    break
                                            else:
                                                # No durations, use simple screens
                                                flow_screens = unique_screens
                                                # print(f" No UI events in time range, using simple screens for {txn_label}")
                                                break
                                        else:
                                            # No columns found, use simple screens
                                            flow_screens = unique_screens
                                            # print(f" Columns not found, using simple screens for {txn_label}")
                                            break
                                    except Exception as e:
                                        # print(f" Could not add durations for {txn_label}: {e}")
                                        flow_screens = unique_screens
                                        break
                                else:
                                    logger.warning(f" No screens found for {txn_label}")
                        else:
                            logger.warning(f" Empty UI journal for {txn_label}")
                    
                    return flow_screens
                
                # Extract flows for both transactions
                ui_flow_1 = extract_flow_with_durations(txn1_data, txn1_source_file, "Transaction 1")
                ui_flow_2 = extract_flow_with_durations(txn2_data, txn2_source_file, "Transaction 2")
                
            except Exception as e:
                logger.error(f" Error extracting UI flows: {e}", exc_info=True)
        else:
            logger.warning(" No UI journal files available")

        logger.info(f" Transaction 1 flow: {len(ui_flow_1)} screens")
        logger.info(f" Transaction 2 flow: {len(ui_flow_2)} screens")

        # --- Part C: LCS Matching and Analysis ---
        def find_lcs_matches(flow1, flow2):
            """Find screens that appear in the same relative order in both flows using LCS"""
            # Extract screen names
            screens1 = []
            for item in flow1:
                if isinstance(item, dict):
                    screens1.append(item['screen'])
                else:
                    screens1.append(str(item))
            
            screens2 = []
            for item in flow2:
                if isinstance(item, dict):
                    screens2.append(item['screen'])
                else:
                    screens2.append(str(item))
            
            m, n = len(screens1), len(screens2)
            lcs_table = [[0] * (n + 1) for _ in range(m + 1)]
            
            # Fill LCS table
            for i in range(1, m + 1):
                for j in range(1, n + 1):
                    if screens1[i-1] == screens2[j-1]:
                        lcs_table[i][j] = lcs_table[i-1][j-1] + 1
                    else:
                        lcs_table[i][j] = max(lcs_table[i-1][j], lcs_table[i][j-1])
            
            # Backtrack to find which screens are part of LCS
            matches1 = [False] * m
            matches2 = [False] * n
            i, j = m, n
            
            while i > 0 and j > 0:
                if screens1[i-1] == screens2[j-1]:
                    matches1[i-1] = True
                    matches2[j-1] = True
                    i -= 1
                    j -= 1
                elif lcs_table[i-1][j] > lcs_table[i][j-1]:
                    i -= 1
                else:
                    j -= 1
            
            return matches1, matches2
        
        # Get matches
        txn1_matches, txn2_matches = find_lcs_matches(ui_flow_1, ui_flow_2)
        
        # Build response
        response_data = {
            "txn1_id": txn1_id,
            "txn2_id": txn2_id,
            "txn1_type": str(txn1_data.get('Transaction Type', 'Unknown')),
            "txn2_type": str(txn2_data.get('Transaction Type', 'Unknown')),
            "txn1_state": str(txn1_data.get('End State', 'Unknown')),
            "txn2_state": str(txn2_data.get('End State', 'Unknown')),
            "txn1_flow": ui_flow_1,
            "txn2_flow": ui_flow_2,
            "txn1_matches": txn1_matches,
            "txn2_matches": txn2_matches,
            "txn1_log": str(txn1_data.get('Transaction Log', '')),
            "txn2_log": str(txn2_data.get('Transaction Log', '')),
            "has_details_1": isinstance(ui_flow_1[0], dict) if (ui_flow_1 and len(ui_flow_1) > 0 and ui_flow_1[0] != "No screens in time range") else False,
            "has_details_2": isinstance(ui_flow_2[0], dict) if (ui_flow_2 and len(ui_flow_2) > 0 and ui_flow_2[0] != "No screens in time range") else False
        }

        logger.info(" Comparison complete - returning response")
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comparison failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)}")


@router.get("/current-selection")
async def get_current_selection(session_id: str = Query(default=None)):
    """
    Retrieve the currently selected file type(s) for a given session.

    DESCRIPTION:
        This endpoint fetches the user's current selection of file categories
        (e.g., customer journals, ACU files, UI journals) from the session data.
        It helps the frontend or other services know which file types are actively selected
        for further processing, filtering, or analysis.

    PARAMETERS:
        session_id (str, optional):
            The session ID containing the selection state.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict:
            {
                "selected_types": list[str],  # Currently selected file type(s)
                "message": str (optional)     # Informational message if no selection exists
            }

    RAISES:
        HTTPException:
            - 404: If the session does not exist
            - 500: For unexpected errors during retrieval

    NOTES:
        - If no file types have been selected in the session yet, the response
          will contain an empty list and an informational message.
        - Useful for UI components to display or maintain the user's current selection.
    """
    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f" Getting current selection for session: {session_id}")

        if not session_service.session_exists(session_id):
            logger.error(f" No session found for session_id: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found"
            )
        
        session = session_service.get_session(session_id)
        selected_types = session.get('selected_types', [])
        
        if not selected_types:
            logger.info(f"  No file types selected yet for session: {session_id}")
            return {"selected_types": [], "message": "No file types selected yet"}
        
        logger.info(f"Current selection for session {session_id}: {selected_types}")
        return {"selected_types": selected_types}
    
    except Exception as e:
        logger.exception(f" Error retrieving current selection for session {session_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving current selection: {str(e)}"
        )


@router.get("/debug-session")
async def debug_session(session_id: str = Query(default=None)):
    """
    FUNCTION:
        debug_session

    DESCRIPTION:
        Checks and displays the internal structure of the current session.
        This is a development/debug tool to verify what data has been stored
        (file categories, selected types, source files, transactions, etc.)
        after file extraction and processing.

    USAGE:
        result = await debug_session(session_id="123456")

    PARAMETERS:
        session_id (str) :
            The session identifier to inspect. Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                "exists"               : bool  - Whether the session exists
                "message"              : str   - Info message when session missing
                "has_file_categories"  : bool
                "file_categories_keys" : list  - Names of file categories
                "file_counts"          : dict  - Count of files per category
                "selected_types"       : list  - Types of files selected by user
                "extraction_path"      : str | None
                "processed_files_dir"  : str
                "has_transaction_data" : bool
                "has_source_files"     : bool
                "source_file_count"    : int
            }

    RAISES:
        HTTPException :
            - 404 : When session does not exist
            - 500 : On unexpected internal errors
    """

    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f" Debugging session: {session_id}")

        if not session_service.session_exists(session_id):
            logger.warning(f"Session not found: {session_id}")
            return {
                "exists": False,
                "message": "Session not found"
            }
        
        session_data = session_service.get_session(session_id)
        logger.info(f" SUCCESS: Session found:{session_id}, keys: {list(session_data.keys())}")
    
        file_categories = session_data.get('file_categories', {})
        file_counts = {cat: len(files) for cat, files in file_categories.items()} if file_categories else {}
        selected_types = session_data.get('selected_types', [])
        source_files = session_data.get('source_files', [])

        logger.debug(f"File categories: {list(file_categories.keys())}")
        logger.debug(f"File counts: {file_counts}")
        logger.debug(f"Selected types: {selected_types}")
        logger.debug(f"Source file count: {len(source_files)}")

        return {
            "exists": True,
            "has_file_categories": 'file_categories' in session_data,
            "file_categories_keys": list(file_categories.keys()),
            "file_counts": file_counts,
            "selected_types": selected_types,
            "extraction_path": session_data.get('extraction_path', None),
            "processed_files_dir": PROCESSED_FILES_DIR,
            "has_transaction_data": 'transaction_data' in session_data,
            "has_source_files": 'source_files' in session_data,
            "source_file_count": len(source_files)
        }

    except Exception as e:
        logger.exception(f" Error debugging session {session_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging session: {str(e)}"
        )


@router.post("/visualize-individual-transaction-flow", dependencies=[Depends(require_elevated_role)])
async def visualize_individual_transaction_flow(request: TransactionVisualizationRequest,session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    FUNCTION:
        visualize_individual_transaction_flow

    DESCRIPTION:
        Generates a screen-flow visualization for a single ATM transaction by
        analyzing UI journal files within the current session. It extracts
        UI screen transitions based on the transaction's start and end times.

    USAGE:
        result = visualize_individual_transaction_flow(request)

    PARAMETERS:
        request (TransactionVisualizationRequest):
            The request body containing the transaction_id whose UI flow
            is to be visualized.

        session_id (str):
            The active session identifier used to retrieve uploaded and
            processed ZIP data. Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict:
            {
                "transaction_id": str,
                "transaction_type": str,
                "start_time": str,
                "end_time": str,
                "end_state": str,
                "transaction_log": str,
                "source_file": str,
                "ui_flow": list[str],      # Ordered screen names
                "has_flow": bool,          # Whether any UI flow was extracted
                "num_events": int          # Number of screens in the flow
            }

    RAISES:
        HTTPException (404):
            - If no session data exists
            - If the transaction ID is not found

        HTTPException (400):
            - If transaction data is missing but required for visualization

        HTTPException (500):
            - If unexpected errors occur while parsing UI journals or
              generating the flow
    """
    session_id = _resolve_session_id(session_id)
    try:
        transaction_id = request.transaction_id
        logger.info(f"Visualizing flow for transaction: {transaction_id}")

        # Check if session exists
        if not session_service.session_exists(session_id):
            logger.warning(f" No processed ZIP found for session: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No processed ZIP found. Please upload a ZIP file first."
            )

        # Get session data
        session_data = session_service.get_session(session_id)
        transaction_data = session_data.get('transaction_data')

        if not transaction_data:
            logger.warning(f" No transaction data available for session: {session_id}")
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )

        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)

        # Check if transaction exists
        txn_exists = len(df[df['Transaction ID'] == transaction_id]) > 0
        if not txn_exists:
            logger.warning(f" Transaction {transaction_id} not found in session: {session_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found."
            )

        # Get transaction details
        txn_data = df[df['Transaction ID'] == transaction_id].iloc[0]
        logger.info(f" Found transaction: {transaction_id}")

        # Extract UI flow
        ui_flow_screens = ["No flow data"]
        has_flow = False

        # Get file categories and UI journal contents from session
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        ui_journal_contents = session_data.get('ui_journal_contents', {})
        logger.info(f" Found {len(ui_journals)} UI journal file(s)")

        if ui_journals:
            try:
                txn_source_file = str(txn_data.get('Source File', ''))
                logger.info(f" Transaction source file: {txn_source_file}")

                # Use journal_parser.match_journal_file for exact stem matching
                matching_ui_journal = match_journal_file(txn_source_file, ui_journals)
                ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals

                for ui_journal_filename in ui_journals_to_check:
                    logger.debug(f" Parsing UI journal: {ui_journal_filename}")
                    content = ui_journal_contents.get(ui_journal_filename, '')
                    ui_df = parse_ui_journal_from_string(content, ui_journal_filename)

                    if not ui_df.empty:
                        logger.info(f" Parsed {len(ui_df)} UI events")
                        processor = UIJournalProcessor.__new__(UIJournalProcessor)
                        processor.file_path = Path(ui_journal_filename)
                        processor.df = ui_df

                        def parse_time(time_str):
                            if pd.isna(time_str):
                                return None
                            if isinstance(time_str, str):
                                try:
                                    return datetime.strptime(time_str, '%H:%M:%S').time()
                                except:
                                    return None
                            elif hasattr(time_str, 'time'):
                                return time_str.time()
                            return time_str

                        start_time = parse_time(txn_data['Start Time'])
                        end_time = parse_time(txn_data['End Time'])

                        if start_time and end_time:
                            logger.info(f" Time range: {start_time} to {end_time}")
                            try:
                                logger.info(" Extracting flow with durations...")
                                
                                # Get unique screen list (from processor)
                                unique_screens = processor.get_screen_flow(start_time, end_time)
                                
                                if not unique_screens or len(unique_screens) == 0:
                                    logger.info(" No screens found in time range")
                                    continue
                                
                                # print(f" Found {len(unique_screens)} unique screens")
                                
                                # Auto-detect columns
                                time_col = None
                                screen_col = None
                                
                                for col in ['Time', 'time', 'timestamp', 'Timestamp', 'TimeStamp']:
                                    if col in ui_df.columns:
                                        time_col = col
                                        break
                                
                                for col in ['ScreenName', 'Screen', 'screen', 'screen_name']:
                                    if col in ui_df.columns:
                                        screen_col = col
                                        break
                                
                                if not time_col or not screen_col:
                                    raise Exception("Missing required columns")
                                
                                # print(f" Using columns: time='{time_col}', screen='{screen_col}'")
                                
                                # Ensure time column is in time format
                                if ui_df[time_col].dtype == 'object' or str(ui_df[time_col].dtype).startswith('datetime'):
                                    ui_df[time_col] = pd.to_datetime(ui_df[time_col], errors='coerce').dt.time
                                
                                # Get ALL screen events in chronological order
                                ui_filtered = ui_df[
                                    (ui_df[time_col] >= start_time) & 
                                    (ui_df[time_col] <= end_time)
                                ].copy()
                                
                                # print(f" Filtered {len(ui_filtered)} UI events in time range")
                                
                                if len(ui_filtered) > 0:
                                    # Build complete sequence with all occurrences
                                    all_events = []
                                    for idx, row in ui_filtered.iterrows():
                                        screen = str(row.get(screen_col, ''))
                                        time_val = row.get(time_col)
                                        
                                        if screen and not pd.isna(screen):
                                            all_events.append((screen, time_val))
                                    
                                    # print(f" Built sequence of {len(all_events)} screen events")
                                    
                                    # Now map each UNIQUE screen to its time range
                                    # For each unique screen, find first and last occurrence
                                    screen_info = {}
                                    for screen_name in unique_screens:
                                        # Find all occurrences of this screen in the sequence
                                        occurrences = [(s, t) for s, t in all_events if s == screen_name]
                                        
                                        if occurrences:
                                            first_time = occurrences[0][1]
                                            last_time = occurrences[-1][1]
                                            
                                            screen_info[screen_name] = {
                                                'first_time': first_time,
                                                'last_time': last_time
                                            }
                                    
                                    # print(f" Mapped {len(screen_info)} unique screens to time ranges")
                                    
                                    # FIX: Build the flow by consecutively deduplicating all_events.
                                    # This preserves each screen's ACTUAL occurrence timestamp in
                                    # sequence order, so screens visited multiple times (e.g. back-
                                    # navigation to DMMainMenu) each get their own correct timestamp
                                    # rather than always referencing the global first occurrence.
                                    # This prevents negative durations caused by the old screen_info
                                    # dict approach which keyed by name and lost positional context.
                                    deduped_events = []
                                    for (screen, t) in all_events:
                                        if not deduped_events or deduped_events[-1][0] != screen:
                                            deduped_events.append((screen, t))

                                    ui_flow_details = []
                                    for i, (screen_name, time_val) in enumerate(deduped_events):
                                        duration = None
                                        if i < len(deduped_events) - 1:
                                            next_time = deduped_events[i + 1][1]
                                            if time_val and next_time:
                                                try:
                                                    dt1 = datetime.combine(date.today(), first_time)
                                                    dt2 = datetime.combine(date.today(), next_time)
                                                    duration = (dt2 - dt1).total_seconds()
                                                except Exception:
                                                    duration = None

                                        ui_flow_details.append({
                                            'screen': screen_name,
                                            'timestamp': str(time_val) if time_val else '',
                                            'duration': duration
                                        })
                                    
                                    
                                    if ui_flow_details and len(ui_flow_details) > 0:
                                        ui_flow_screens = ui_flow_details
                                        has_flow = True
                                        
                                        with_duration = sum(1 for s in ui_flow_details if s['duration'] is not None)
                                        logger.info(f" Created detailed flow: {len(ui_flow_details)} unique screens, {with_duration} with durations")
                                        
                                        # Debug: print all screens
                                        # for i, screen in enumerate(ui_flow_details):
                                            # dur_str = f"{screen['duration']:.1f}s" if screen['duration'] is not None else "N/A"
                                            # print(f"   {i+1}. {screen['screen']} @ {screen['timestamp']} ({dur_str})")
                                        
                                        break
                                    else:
                                        raise Exception("No screens after processing")
                                else:
                                    raise Exception("No filtered events")
                                    
                            except Exception as e:
                                traceback.print_exc()
                                
                                # Fallback
                                try:
                                    simple_screens = processor.get_screen_flow(start_time, end_time)
                                    if simple_screens and len(simple_screens) > 0:
                                        ui_flow_screens = simple_screens
                                        has_flow = True
                                        # print(f" Using fallback: {len(simple_screens)} screens")
                                        break
                                except:
                                    continue
                        else:
                            logger.warning(" Invalid time range")
                    else:
                        logger.warning(f" Empty UI journal")
                        
            except Exception as e:
                logger.exception(f" Error extracting UI flow: {str(e)}")
        else:
            logger.warning(" No UI journal files available")

        response_data = {
            "transaction_id": transaction_id,
            "transaction_type": str(txn_data.get('Transaction Type', 'Unknown')),
            "start_time": str(txn_data.get('Start Time', '')),
            "end_time": str(txn_data.get('End Time', '')),
            "end_state": str(txn_data.get('End State', 'Unknown')),
            "transaction_log": str(txn_data.get('Transaction Log', '')),
            "source_file": str(txn_data.get('Source File', 'Unknown')),
            "ui_flow": ui_flow_screens,
            "has_flow": has_flow,
            "has_details": isinstance(ui_flow_screens[0], dict) if (ui_flow_screens and len(ui_flow_screens) > 0 and ui_flow_screens[0] != "No flow data") else False,
            "num_events": len(ui_flow_screens) if ui_flow_screens else 0
        }

        logger.info(" Visualization data prepared")
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f" Visualization failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Visualization failed: {str(e)}"
        )

@router.post("/generate-consolidated-flow", dependencies=[Depends(require_elevated_role)])
async def generate_consolidated_flow(source_file: str = Body(...),transaction_type: str = Body(...),session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
FUNCTION:
    generate_consolidated_flow

DESCRIPTION:
    Generates a consolidated UI flow visualization for all transactions of a
    specific transaction type originating from a given source file.
    The function filters transaction data, identifies the correct UI journal,
    extracts UI screen navigation flows for every matching transaction, and
    aggregates these results to produce a combined flow map containing:
    - unique screens visited
    - screen transitions and frequency
    - screens involved in each transaction
    - detailed per-transaction UI flows

USAGE:
    result = generate_consolidated_flow(
        source_file="ATM1",
        transaction_type="Cash Withdrawal",
        session_id="12345"
    )

PARAMETERS:
    source_file (str):
        File name (without extension) of the UI journal associated with the
        transaction data. Used to match transaction source to the UI log file.

    transaction_type (str):
        Specifies which type of transaction should be analyzed
        (e.g., "Cash Withdrawal", "Balance Inquiry", etc.).

    session_id (str):
        Unique session ID used to retrieve previously processed ZIP data,
        including UI journals and parsed transaction data. Defaults to the
        currently active session.

RETURNS:
    dict:
        A structured dictionary containing the consolidated UI flow:
        {
            "source_file": str,
            "transaction_type": str,
            "total_transactions": int,
            "transactions_with_flow": int,
            "successful_count": int,
            "unsuccessful_count": int,
            "screens": list[str],                   # unique screens visited
            "transitions": [
                {
                    "from": str,
                    "to": str,
                    "count": int
                }
            ],
            "screen_transactions": {
                "ScreenName": [
                    {
                        "txn_id": str,
                        "start_time": str,
                        "state": str
                    }
                ]
            },
            "transaction_flows": {
                "txn_id": {
                    "screens": list[str],
                    "start_time": str,
                    "end_time": str,
                    "state": str
                }
            }
        }

RAISES:
    HTTPException 404:
        - Session not found
        - No transactions of the given type found in the specified source file
        - No matching UI journal found for the source file

    HTTPException 400:
        - UI journal could not be parsed or is empty
        - Missing transaction data in session

    HTTPException 500:
        - Unexpected internal errors during UI flow extraction or processing
"""

    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f" Starting consolidated flow generation for type '{transaction_type}' from source '{source_file}'")
        logger.debug(f"Session ID received: {session_id}")
    
        logger.info(f" Generating consolidated flow for {transaction_type} from {source_file}")
        
        # Check session
        if not session_service.session_exists(session_id):
            logger.error("Session does not exist")
            raise HTTPException(
                status_code=404,
                detail="No session found"
            )
        
        logger.debug("Session exists. Fetching session data.")
        session_data = session_service.get_session(session_id)
        
        # Get transaction data
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            logger.error("Transaction data missing in session")
            raise HTTPException(
                status_code=400,
                detail="No transaction data available"
            )
        
        logger.info("Converting transaction data to DataFrame")
        df = pd.DataFrame(transaction_data)
        
        # Filter by source file and transaction type
        logger.debug(f"Applying filters Source File: {source_file}, Transaction Type: {transaction_type}")
        filtered_df = df[
            (df['Source File'] == source_file) & 
            (df['Transaction Type'] == transaction_type)
        ]
        
        if len(filtered_df) == 0:
            logger.error("No matching transactions found after filtering")
            raise HTTPException(
                status_code=404,
                detail=f"No transactions of type '{transaction_type}' found in source '{source_file}'"
            )
        
        
        logger.info(f" Found {len(filtered_df)} transactions")
        
        # Get UI journals and contents from session
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        ui_journal_contents = session_data.get('ui_journal_contents', {})

        logger.debug("Searching for matching UI journal")
        matching_ui_journal = None
        for ui_journal in ui_journals:
            if Path(ui_journal).stem == source_file:
                matching_ui_journal = ui_journal
                break

        if not matching_ui_journal:
            logger.error("No matching UI journal found for this source")
            raise HTTPException(
                status_code=404,
                detail=f"No matching UI journal found for source '{source_file}'"
            )

        logger.info(f"Matched UI journal: {matching_ui_journal}")

        # Parse UI journal from session memory
        logger.info("Parsing UI journal from session memory")
        content = ui_journal_contents.get(matching_ui_journal, '')
        ui_df = parse_ui_journal_from_string(content, matching_ui_journal)

        if ui_df.empty:
            logger.error("Parsed UI journal is empty")
            raise HTTPException(
                status_code=400,
                detail="UI journal is empty or could not be parsed"
            )

        logger.info(f"UI journal parsed successfully with {len(ui_df)} events")

        # Create processor from pre-parsed df (no disk read needed)
        logger.debug("Creating UIJournalProcessor instance")
        processor = UIJournalProcessor.__new__(UIJournalProcessor)
        processor.file_path = Path(matching_ui_journal)
        processor.df = ui_df
        
        # Extract flows
        logger.info("Extracting UI flows for each transaction")
        transaction_flows = {}
        all_screens = set()
        transitions = defaultdict(int)
        screen_transactions = defaultdict(list)
        
        for _, txn in filtered_df.iterrows():
            txn_id = txn['Transaction ID']
            logger.debug(f"Processing transaction ID: {txn_id}")
            
            # Parse times
            def parse_time(time_str):
                if pd.isna(time_str):
                    return None
                if isinstance(time_str, str):
                    try:
                        return datetime.strptime(time_str, '%H:%M:%S').time()
                    except:
                        return None
                elif hasattr(time_str, 'time'):
                    return time_str.time()
                return time_str
            
            start_time = parse_time(txn['Start Time'])
            end_time = parse_time(txn['End Time'])
            
            logger.debug(f"Times parsed  Start: {start_time}, End: {end_time}")
            
            if start_time and end_time:
                screens = processor.get_screen_flow(start_time, end_time)
                
                if screens and len(screens) > 0:
                    logger.debug(f"Extracted {len(screens)} screens for txn {txn_id}")
                    transaction_flows[txn_id] = {
                        'screens': screens,
                        'start_time': str(start_time),
                        'end_time': str(end_time),
                        'state': txn['End State']
                    }
                    
                    for screen in screens:
                        all_screens.add(screen)
                        screen_transactions[screen].append({
                            'txn_id': txn_id,
                            'start_time': str(start_time),
                            'state': txn['End State']
                        })
                    
                    for i in range(len(screens) - 1):
                        transitions[(screens[i], screens[i + 1])] += 1
        
        if not transaction_flows:
            logger.error("UI flow extraction failed no flows found")
            raise HTTPException(
                status_code=404,
                detail="No UI flow data could be extracted for these transactions"
            )
        
        logger.info(f" Extracted flows for {len(transaction_flows)} transactions")
        logger.debug(f"Unique screens: {len(all_screens)}, Unique transitions: {len(transitions)}")
        
        
        
        # Prepare response
        logger.info("Preparing response payload")
        response_data = {
            "source_file": source_file,
            "transaction_type": transaction_type,
            "total_transactions": len(filtered_df),
            "transactions_with_flow": len(transaction_flows),
            "successful_count": len(filtered_df[filtered_df['End State'] == 'Successful']),
            "unsuccessful_count": len(filtered_df[filtered_df['End State'] == 'Unsuccessful']),
            "screens": list(all_screens),
            "transitions": [
                {
                    "from": from_screen,
                    "to": to_screen,
                    "count": count
                }
                for (from_screen, to_screen), count in transitions.items()
            ],
            "screen_transactions": {
                screen: txns
                for screen, txns in screen_transactions.items()
            },
            "transaction_flows": transaction_flows
        }
        
        logger.info("Consolidated flow generation completed successfully")
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected failure: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate consolidated flow: {str(e)}"
        )
    


class TransactionAnalysisRequest(BaseModel):
    transaction_id: str
    employee_code: str


@router.post("/analyze-transaction-llm")
async def analyze_transaction_llm(request: TransactionAnalysisRequest, session_id: str = Query(default=None)):
    """
    FUNCTION:
        analyze_transaction_llm

    DESCRIPTION:
        Analyzes a single ATM transaction using the LLM. Validates the session
        and transaction data, then delegates the full EJ/JRN pipeline, prompt
        construction, Ollama call, and metadata storage to llm_service.analyze_transaction().

    USAGE:
        result = await analyze_transaction_llm(request, session_id="current_session")

    PARAMETERS:
        request (TransactionAnalysisRequest) :
            Contains transaction_id and employee_code.
        session_id (str) :
            Session ID containing processed transaction data.
            Defaults to CURRENT_SESSION_ID.

    RETURNS:
        dict :
            {
                "summary": str,
                "analysis": str,
                "timestamp": str,
                "metadata": {
                    "transaction_id": str,
                    "model": str,
                    "log_length": int,
                    "response_length": int,
                    "analysis_type": str,
                    "transaction_type": str,
                    "transaction_state": str,
                    "start_time": str,
                    "end_time": str,
                    "source_file": str,
                    "jrn_data_available": bool,
                    "analysis_time_seconds": float
                }
            }

    RAISES:
        HTTPException :
            - 404 if session or transaction not found
            - 400 if no transaction log available
            - 422 if log could not be parsed or has insufficient data
            - 500 if Ollama is not installed or LLM call fails
    """
    session_id = _resolve_session_id(session_id)
    try:
        transaction_id = request.transaction_id
        logger.info(f" Analyzing transaction with LLM: {transaction_id}")
        logger.debug(f"Request data: {request.dict()}")

        # ── Session check ─────────────────────────────────────────────────
        if not session_service.session_exists(session_id):
            logger.error(f"No session found for session_id: {session_id}")
            raise HTTPException(status_code=404, detail="No session found")

        session_data = session_service.get_session(session_id)
        logger.info(f"Session data retrieved for session_id {session_id}")

        # ── Transaction data ──────────────────────────────────────────────
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            logger.error(f"No transaction data available for session_id: {session_id}")
            raise HTTPException(status_code=400, detail="No transaction data available")

        df = pd.DataFrame(transaction_data)

        if transaction_id not in df['Transaction ID'].values:
            logger.error(f"Transaction {transaction_id} not found in session {session_id}")
            raise HTTPException(status_code=404, detail=f"Transaction {transaction_id} not found")

        txn_data        = df[df['Transaction ID'] == transaction_id].iloc[0].to_dict()
        transaction_log = str(txn_data.get('Transaction Log', ''))
        if not transaction_log:
            logger.error(f"No transaction log available for transaction {transaction_id}")
            raise HTTPException(status_code=400, detail="No transaction log available for this transaction")

        logger.info(f" Found transaction log ({len(transaction_log)} characters)")

        # ── Filter UI journal filenames (exclude VCP-PRO path) ────────────
        file_categories = session_data.get('file_categories', {})
        all_ui_journals = file_categories.get('ui_journals', [])
        llm_ui_journals = [
            f for f in all_ui_journals
            if 'vcp-pro' not in str(f).replace('\\', '/').lower()
        ]

        # ── Collect JRN + CUSTOMER contents from session memory ───────────
        # The temp extraction folder is already deleted by this point, so
        # the LLM must read all file data from session memory.
        ui_journal_contents       = session_data.get('ui_journal_contents', {})
        journal_llm_contents      = session_data.get('journal_llm_contents', {})
        all_jrn_contents          = {**ui_journal_contents, **journal_llm_contents}
        customer_journal_contents = session_data.get('customer_journal_contents', {})

        logger.info(
            f"[LLM-DEBUG] all_ui_journals={all_ui_journals} | "
            f"llm_ui_journals={llm_ui_journals} | "
            f"ui_journal_contents keys={list(ui_journal_contents.keys())} | "
            f"journal_llm_contents keys={list(journal_llm_contents.keys())} | "
            f"customer_journal_contents keys={list(customer_journal_contents.keys())}"
        )

        source_file = str(txn_data.get('Source File', ''))
        has_jrn_proto = bool(txn_data.get('JRN Protocol Steps'))
        has_jrn_errors = bool(txn_data.get('JRN Device Errors'))
        logger.info(
            f"[LLM-DEBUG] txn Source File={source_file} | "
            f"txn has JRN Protocol Steps={has_jrn_proto} | "
            f"txn has JRN Device Errors={has_jrn_errors}"
        )

        # ── Delegate full LLM pipeline to llm_service ─────────────────────
        return analyze_transaction(
            transaction_id=transaction_id,
            transaction_log=transaction_log,
            txn_data=txn_data,
            ui_journal_files=llm_ui_journals,
            ui_journal_contents=all_jrn_contents,
            customer_journal_contents=customer_journal_contents,
            employee_code=request.employee_code,
        )

    except HTTPException:
        raise
    except ValueError as ve:
        logger.error(f"Validation error for transaction {request.transaction_id}: {ve}")
        raise HTTPException(status_code=422, detail=str(ve))
    except ImportError:
        logger.error("Ollama is not installed")
        raise HTTPException(
            status_code=500,
            detail="Ollama is not installed. Please install it with: pip install ollama"
        )
    except Exception as e:
        logger.exception(f"Analysis failed for transaction {request.transaction_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )


# Add this Pydantic model near the top with other models
class FeedbackSubmission(BaseModel):
    model_config = {'protected_namespaces': ()}

    transaction_id: str
    rating: int
    alternative_cause: str
    comment: str
    user_name: str
    user_email: str
    model_version: str
    original_llm_response: str

@router.post("/submit-llm-feedback")
async def submit_llm_feedback(
    feedback: FeedbackSubmission,
    session_id: str = Query(default=CURRENT_SESSION_ID),
    authorization: str = Header(default=None),
):
    """
    FUNCTION:
        submit_llm_feedback

    DESCRIPTION:
        Handles submission of user feedback related to LLM-generated analysis for a
        specific transaction. The function logs the feedback, stores it in a local
        JSONL file for auditability, and keeps a copy in the active session for
        immediate availability in the UI or further processing.

    USAGE:
        result = await submit_llm_feedback(
            feedback=FeedbackSubmission(...),
            session_id="12345"
        )

    PARAMETERS:
        feedback (FeedbackSubmission):
            Pydantic model containing all feedback fields submitted by the user,
            including transaction ID, rating, alternative cause, comments,
            user identity, model version, and the original LLM response.

        session_id (str):
            Unique session identifier used to store and organize feedback within
            session storage. Defaults to the current active session.

        authorization (str):
            Bearer token from the Authorization header. Required — requests
            without a valid token are rejected with 401.

    RETURNS:
        dict:
            {
                "status": "success",
                "message": "Thank you <name>! Your feedback has been recorded.",
                "timestamp": "<YYYY-MM-DD HH:MM:SS>"
            }

    SIDE EFFECTS:
        - Appends feedback as a JSON line in `llm_feedback.json`
        - Stores feedback in session under `feedback_data` list

    RAISES:
        HTTPException 401: Missing or invalid/expired JWT.
        HTTPException 403: Role not permitted to submit feedback (ADMIN blocked).
        HTTPException 429: Feedback already submitted for this transaction.
        HTTPException 500: Unexpected failure during storage.

    SECURITY:
        - JWT is mandatory; there is no unauthenticated fallback path.
        - Role and username are derived exclusively from the verified JWT payload.
        - feedback.user_name from the request body is NOT trusted for auth decisions.
        - No DB role lookup fallback — prevents body-injection bypass.
    """

    # ── Step 1: Token is mandatory — reject immediately if absent ──────────────
    if not authorization or not authorization.startswith("Bearer "):
        logger.warning("FEEDBACK [401] — missing or malformed Authorization header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "status": "error",
                "code": 401,
                "error": "Unauthorized",
                "message": "Authentication token missing. Please log in to submit feedback.",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    # ── Step 2: Decode and validate JWT — decode_access_token raises 401 if bad ─
    token = authorization.split(" ", 1)[1]
    payload = decode_access_token(token)

    # ── Step 3: Ensure required claims are present in the token ────────────────
    jwt_role     = payload.get("role" or "").strip()
    jwt_username = payload.get("sub" or "").strip()

    if not jwt_role or not jwt_username:
        logger.warning(
            "FEEDBACK [401] — JWT missing required claims: role='%s' sub='%s'",
            jwt_role, jwt_username,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "status": "error",
                "code": 401,
                "error": "Unauthorized",
                "message": "Malformed token. Please log in again.",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    # ── Step 4: Role-based access control — no DB fallback ─────────────────────
    jwt_role_upper = jwt_role.upper()

    if jwt_role_upper == "ADMIN":
        logger.warning(
            "FEEDBACK [403] — user='%s' role='ADMIN' attempted to submit feedback (blocked)",
            jwt_username,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="ADMIN role is not permitted to submit feedback.",
        )

    if jwt_role_upper not in ("USER", "DEV_MODE"):
        logger.warning(
            "FEEDBACK [403] — user='%s' role='%s' is not an allowed feedback role",
            jwt_username, jwt_role_upper,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only users with USER or DEV_MODE roles can submit feedback.",
        )

    # ── Step 5: Use verified identity from JWT, not from request body ──────────
    # jwt_username is the authoritative user identity for all storage operations.
    # feedback.user_name is kept only for the display message below.
    verified_username = jwt_username

    session_id = _resolve_session_id(session_id)
    try:
        logger.info(
            "Submitting feedback — txn: %s  user: %s  role: %s",
            feedback.transaction_id, verified_username, jwt_role_upper,
        )
        logger.debug("Feedback payload: %s", feedback.dict())

        # ── Build feedback record ───────────────────────────────────────────────
        now = datetime.now()
        feedback_record = {
            "transaction_id":       feedback.transaction_id,
            "rating":               feedback.rating,
            "alternative_cause":    feedback.alternative_cause,
            "comment":              feedback.comment,
            "user_name":            verified_username,          # from JWT, not body
            "user_email":           feedback.user_email,
            "model_version":        feedback.model_version,
            "original_llm_response": feedback.original_llm_response,
            "timestamp":            now.strftime("%Y-%m-%d %H:%M:%S"),
            "submission_date":      now.strftime("%Y-%m-%d"),
            "submission_time":      now.strftime("%H:%M:%S"),
            "session_id":           session_id,
        }

        # ── Persist to audit file ───────────────────────────────────────────────
        feedback_file = Path("llm_feedback.json")
        try:
            with open(feedback_file, "a") as f:
                f.write(json.dumps(feedback_record) + "\n")
            logger.info("Feedback saved to file: %s", feedback_file)
        except Exception as e:
            logger.error("Could not save feedback to file %s: %s", feedback_file, e)

        # ── Persist to database ─────────────────────────────────────────────────
        result = store_feedback(
            transaction_id    = feedback.transaction_id,
            user_name         = verified_username,              # from JWT, not body
            rating            = feedback.rating,
            alternative_cause = feedback.alternative_cause,
            comment           = feedback.comment,
            model_version     = feedback.model_version,
        )

        if result == "LIMIT_REACHED":
            raise HTTPException(
                status_code=429,
                detail="You have already submitted feedback for this transaction.",
            )

        # ── Store in session for immediate UI retrieval ─────────────────────────
        session_data = session_service.get_session(session_id)
        if "feedback_data" not in session_data:
            session_data["feedback_data"] = []
        session_data["feedback_data"].append(feedback_record)
        session_service.update_session(session_id, "feedback_data", session_data["feedback_data"])
        logger.info("Feedback stored in session: %s", session_id)

        return {
            "status":    "success",
            "message":   f"Thank you {verified_username}! Your feedback has been recorded.",
            "timestamp": feedback_record["timestamp"],
        }

    except HTTPException:
        raise

    except Exception as e:
        logger.exception(
            "Failed to submit feedback for transaction %s: %s",
            feedback.transaction_id, e,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit feedback: {str(e)}",
        )

@router.get("/get-feedback/{transaction_id}")
async def get_feedback(transaction_id: str,session_id: str = Query(default=None)):
    """
FUNCTION:
    get_feedback

DESCRIPTION:
    Retrieves all feedback entries associated with a specific transaction ID.
    This function searches both the active session storage and the persistent
    `llm_feedback.json` file to consolidate and return all matching feedback
    records. Duplicate entries are automatically filtered out.

USAGE:
    result = await get_feedback(
        transaction_id="TXN12345",
        session_id="12345"
    )

PARAMETERS:
    transaction_id (str):
        The unique identifier of the transaction for which the user wants to
        retrieve feedback.

    session_id (str):
        Session identifier used to retrieve in-session feedback data. Defaults
        to the current active session.

RETURNS:
    dict:
        {
            "transaction_id": str,
            "feedback_count": int,
            "feedback": list[dict]
        }

        - `transaction_id`: The transaction ID used in the query.
        - `feedback_count`: Total number of feedback entries found.
        - `feedback`: List of feedback objects containing fields such as
            - transaction_id
            - rating
            - alternative_cause
            - comment
            - user_name
            - user_email
            - model_version
            - original_llm_response
            - timestamp
            - session_id

DATA SOURCES:
    - Session storage (`feedback_data` list)
    - `llm_feedback.json` file (JSONL format)

SIDE EFFECTS:
    None this endpoint only reads data.

RAISES:
    HTTPException 500:
        - Triggered when an unexpected error occurs while reading file data,
          parsing JSON, or retrieving session information.
"""

    session_id = _resolve_session_id(session_id)
    try:
        logger.info(f" Retrieving feedback for transaction: {transaction_id}")
        
        all_feedback = []
        
        # Get from session
        if session_service.session_exists(session_id):
            session_data = session_service.get_session(session_id)
            session_feedback = session_data.get('feedback_data', [])
            logger.debug(f"Retrieved {len(session_feedback)} feedback records from session {session_id}")
            
            # Filter by transaction ID
            filtered_feedback = [
                f for f in session_feedback 
                if f.get('transaction_id') == transaction_id
            ]
            all_feedback.extend(filtered_feedback)
            logger.debug(f"{len(filtered_feedback)} feedback records match the transaction ID in session")
        
        # Also read from file
        feedback_file = Path("llm_feedback.json")
        if feedback_file.exists():
            try:
                with open(feedback_file, "r") as f:
                    for line in f:
                        if line.strip():
                            feedback_record = json.loads(line)
                            if feedback_record.get('transaction_id') == transaction_id:
                                # Avoid duplicates
                                if feedback_record not in all_feedback:
                                    all_feedback.append(feedback_record)
                logger.info(f"Read feedback from file {feedback_file}, total records found: {len(all_feedback)}")
            except Exception as e:
                logger.error(f" Could not read feedback file {feedback_file}: {str(e)}")
        
        logger.info(f" Found {len(all_feedback)} feedback record(s) for transaction {transaction_id}")
        
        return {
            "transaction_id": transaction_id,
            "feedback_count": len(all_feedback),
            "feedback": all_feedback
        }
        
    except Exception as e:
        logger.exception(f"Failed to retrieve feedback for transaction {transaction_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve feedback: {str(e)}"
        )
    

# ============================================
# GET ANALYSIS RECORDS
# ============================================

@router.get("/get-analysis-records", dependencies=[Depends(require_elevated_role)])
async def fetch_analysis_records(
    transaction_id: str = Query(..., description="Transaction ID to look up"),
    employee_code:  str = Query(..., description="Employee code of logged in user"),
):
    """
        Retrieve analysis records for a specific transaction and employee.

        Endpoint:
        ---------
        GET /get-analysis-records

        Description:
        ------------
        Fetches all analysis records from the database that match the
        provided transaction_id and employee_code.

        This endpoint is typically used by the frontend to display
        previously generated LLM analysis results for a logged-in user.

        Query Parameters:
        -----------------
        transaction_id : str (required)
            Unique transaction identifier to search for.

        employee_code : str (required)
            Employee code of the authenticated user.
            Ensures users can only access their own records.

        Returns:
        --------
        200 OK:
            {
                "status": "success",
                "count": int,
                "records": [
                    {
                        "transaction_id": str,
                        "employee_code": str,
                        "model": str,
                        "transaction_type": str,
                        "transaction_state": str,
                        "source_file": str,
                        "start_time": str,
                        "end_time": str,
                        "log_length": int,
                        "response_length": int,
                        "analysis_time_seconds": float,
                        "llm_analysis": str,
                        "created_at": datetime
                    }
                ]
            }

        404 Not Found:
            Raised when no matching records are found.

        Raises:
        -------
        HTTPException:
            - 404 if no records exist for the given transaction_id.

        Logging:
        --------
        Logs transaction_id and employee_code for traceability.

        Security Note:
        --------------
        employee_code should ideally be derived from authentication
        context (e.g., JWT/session) rather than passed as a query parameter.
    """
    
    logger.info(f"Fetching analysis records — txn: {transaction_id}, emp: {employee_code}")

    records = get_analysis_records(
        transaction_id = transaction_id,
        employee_code  = employee_code,
    )

    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No records found for transaction_id='{transaction_id}'"
        )

    return {
        "status": "success",
        "count":   len(records),
        "records": records
    }

# ============================================
# GET FEEDBACK RECORDS
# ============================================

@router.get("/get-feedback-records", dependencies=[Depends(require_elevated_role)])
async def fetch_feedback_records(
    transaction_id: str = Query(..., description="Transaction ID"),
    user_name:      str = Query(..., description="Logged in username")
):
    """
        Retrieve feedback records for a specific transaction and user.

        Endpoint:
        ---------
        GET /get-feedback-records

        Description:
        ------------
        Fetches all feedback entries associated with the given
        transaction_id and user_name from the database.

        This endpoint allows users to view their previously submitted
        feedback related to a specific transaction analysis.

        Query Parameters:
        -----------------
        transaction_id : str (required)
            Unique identifier of the transaction.

        user_name : str (required)
            Username of the logged-in user.
            Used to ensure users only access their own feedback records.

        Returns:
        --------
        200 OK:
            {
                "status": "success",
                "count": int,
                "records": [
                    {
                        "transaction_id": str,
                        "user_name": str,
                        "rating": int,
                        "alternative_cause": str,
                        "comment": str,
                        "model_version": str,
                        "submitted_at": datetime
                    }
                ]
            }

        404 Not Found:
            Raised when no feedback records exist for the given transaction_id.

        Raises:
        -------
        HTTPException:
            - 404 if no feedback is found.

        Logging:
        --------
        Logs transaction_id and user_name for monitoring and traceability.

        Security Note:
        --------------
        user_name should ideally be derived from authenticated session
        context (e.g., JWT token or session middleware) instead of being
        passed as a query parameter.
    """
    logger.info(f"Fetching feedback records — txn: {transaction_id}, user: {user_name}")

    records = get_feedback_records(
        transaction_id = transaction_id,
        user_name      = user_name
    )

    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No feedback found for transaction_id='{transaction_id}'"
        )

    return {
        "status":  "success",
        "count":   len(records),
        "records": records
    }


# ============================================
# FORGOT PASSWORD ENDPOINTS
# ============================================
class ForgotPasswordRequest(BaseModel):
    username:      str
    employee_code: str
    base_url:      str  # actual app URL detected from browser, passed by Streamlit UI

class ResetPasswordRequest(BaseModel):
    token:            str
    new_password:     str
    confirm_password: str


# ─────────────────────────────────────────────────────────────────────────────
# POST /forgot-password
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/forgot-password")
async def forgot_password(request: ForgotPasswordRequest):
    """
        Handles the Forgot.
        Endpoint:
        ---------
        POST /api/v1/forgot-password

        Description:
        ------------
        1. Verifies that the username + employee_code combination
           exists in the DB and the account is active.
        2. If valid, generates a secure single-use reset token and
           stores it in the password_reset_tokens table.
        3. Sends a password reset email to the user containing a
           link built from the base_url passed by the frontend.

        Request Body:
        -------------
        {
            "username":      "user@example.com",
            "employee_code": "12345678",
            "base_url":      "http://192.168.1.5:8501"
        }

        base_url is the actual URL the Streamlit app is being accessed
        from (detected via st.context.headers on the frontend). This
        ensures the reset link in the email works on any machine on the
        network, not just localhost.

        Returns:
        --------
        200 OK:
            { "status": "success", "message": "Reset email sent." }

        400 Bad Request:
            { "detail": "Invalid username or employee code." }

        500 Internal Server Error:
            { "detail": "Could not send reset email." }

        Security:
        ---------
        - Returns generic 400 for invalid identity (no info leak).
        - Token is single-use and expires in 30 minutes.
        - Previous active tokens for the user are invalidated.
    """
    logger.info("send forgot-password— user: %s", request.username)

    # ── Step 1: Verify identity ────────────────────────────────────
    is_valid = verify_reset_identity(
        username=request.username.strip(),
        employee_code=request.employee_code.strip()
    )

    if not is_valid:
        logger.warning(
            "forgot_password: identity check failed for user: %s",
            request.username
        )
        raise HTTPException(
            status_code=400,
            detail="Invalid username or employee code. "
                   "Please check your details and try again."
        )

    # ── Step 2: Generate token ─────────────────────────────────────
    token = generate_reset_token(request.username.strip())

    if not token:
        logger.error(
            "forgot_password: token generation failed for user: %s",
            request.username
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to generate reset token. Please try again."
        )

    # ── Step 3: Send email with real base_url ──────────────────────
    email_sent = send_reset_email(
        to_email=request.username.strip(),
        token=token,
        base_url=request.base_url.strip() if request.base_url else None
    )

    if not email_sent:
        logger.error(
            "forgot_password: email failed for user: %s",
            request.username
        )
        raise HTTPException(
            status_code=500,
            detail="Identity verified but email could not be sent. "
                   "Please contact your administrator."
        )

    logger.info(
        "forgot_password: reset email dispatched for user: %s via %s",
        request.username, request.base_url
    )
    return {
        "status":  "success",
        "message": "A password reset link has been sent to your email. "
                   "Please check your inbox and click the link within 30 minutes."
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /validate-reset-token
# Step: Check if a token is still valid before showing the reset form
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/validate-reset-token")
async def validate_token_endpoint(token: str = Query(..., description="Reset token from email link")):
    """
        Validates a password reset token.

        Endpoint:
        ---------
        GET /api/v1/validate-reset-token?token=<token>

        Description:
        ------------
        Checks whether the given token:
        - Exists in the password_reset_tokens table.
        - Has not been used (is_used = FALSE).
        - Has not expired (expires_at > NOW()).

        Called by the Streamlit frontend when the user lands on the
        reset password page, before showing the new password form.

        Query Parameters:
        -----------------
        token : str (required)
            The URL-safe reset token from the email link.

        Returns:
        --------
        200 OK (valid):
            { "status": "valid", "username": "user@example.com" }

        400 Bad Request (invalid/expired):
            { "detail": "Reset link is invalid or has expired." }

        Security:
        ---------
        Does not reveal WHY the token is invalid (expired vs used).
    """
    logger.info("validate-reset-token called")

    username = validate_reset_token(token)

    if not username:
        logger.warning("validate_token_endpoint: invalid/expired token")
        raise HTTPException(
            status_code=400,
            detail="This reset link is invalid or has expired. "
                   "Please request a new password reset."
        )

    logger.info("validate_token_endpoint: valid token for user: %s", username)
    return {
        "status":   "valid",
        "username": username
    }


# ─────────────────────────────────────────────────────────────────────────────
# POST /reset-password
# Step : Validate all  rules + update password + consume token
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/reset-password")
async def reset_password_endpoint(request: ResetPasswordRequest):
    """
        Resets the user's password after full validation.

        Endpoint:
        ---------
        POST /api/v1/reset-password

        Description:
        ------------
        Applies all 4 password validation rules before updating:

        Rule 1 — Both fields filled:
            new_password and confirm_password must not be empty.

        Rule 2 — Passwords match:
            new_password must equal confirm_password exactly.

        Rule 3 — Strong password (is_valid_password):
            - Minimum 8 characters
            - At least 1 uppercase letter
            - At least 1 lowercase letter
            - At least 2 digits
            - At least 1 special character

        Rule 4 — Not same as old password:
            New password hash must differ from the current stored hash.

        Then:
        - Updates password_hash in Users table.
        - Marks the token as is_used = TRUE (single-use).
        - Logs a password_reset event in login_history.

        Request Body:
        -------------
        {
            "token":            "<reset token>",
            "new_password":     "NewPass@123",
            "confirm_password": "NewPass@123"
        }

        Returns:
        --------
        200 OK:
            { "status": "success", "message": "Password reset successfully." }

        400 Bad Request:
            { "detail": "<specific validation error message>" }

        Security:
        ---------
        - Token is validated and consumed atomically.
        - Password is SHA-256 hashed before storage.
        - Expired/used tokens are rejected at this stage too.
    """
    logger.info("POST /reset-password called")

    if not request.new_password or not request.confirm_password:
        raise HTTPException(
            status_code=400,
            detail="Both password fields are required."
        )
    if request.new_password != request.confirm_password:
        raise HTTPException(
            status_code=400,
            detail="Passwords do not match. Please re-enter both fields."
        )
    if not is_valid_password(request.new_password):
        raise HTTPException(
            status_code=400,
            detail=(
                "Password does not meet strength requirements: "
                "min 8 characters, 1 uppercase, 1 lowercase, "
                "2 digits, 1 special character (!@#$%^&* etc.)"
            )
        )

    # ── Rule 4: Token validation + old password check + DB update ─
    success, message = reset_user_password(
        token=request.token,
        new_password=request.new_password
    )

    if not success:
        logger.warning("reset_password_endpoint: reset failed — %s", message)
        raise HTTPException(status_code=400, detail=message)

    logger.info("reset_password_endpoint: password reset successful")
    return {
        "status":  "success",
        "message": "Your password has been reset successfully. You can now log in."
    }


# ============================================
# AUTH ENDPOINTS
# ============================================

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    username: str
    name: str | None = None
    employee_code: str
    role: str
    session_token: str

class RegisterRequest(BaseModel):
    email: str
    name: str
    password: str
    employee_code: str
    role: str = "USER"

class LogoutRequest(BaseModel):
    username: str


@router.post("/auth/login", response_model=LoginResponse)
async def auth_login(request: LoginRequest):
    """
    Verify username + password against the Users table.
    Returns user info + signed JWT session_token on success (200).
    Returns 401 for wrong credentials.
    Returns 403 if account exists but is pending admin approval.
    """
    logger.info("POST /auth/login: user: %s", request.username)

    user = authenticate_user_backend(request.username.strip(), request.password)

    if user:
        log_login_event(username=user["username"], action="login")
        logger.info("Login successful: user=%s role=%s", user["username"], user.get("role"))
        token = create_access_token(
            username=user["username"],
            role=user.get("role", "USER"),
            employee_code=user.get("employee_code", ""),
        )
        return LoginResponse(
            username=user["username"],
            name=user.get("name"),
            employee_code=user.get("employee_code", ""),
            role=user.get("role", "USER"),
            session_token=token,
        )

    if is_user_pending_approval(request.username.strip(), request.password):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your account is pending admin approval. "
                   "Please contact your administrator to activate your account.",
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid username or password.",
    )


@router.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def auth_register(request: RegisterRequest):
    """Create a new user account (inactive by default)."""
    logger.info("POST /auth/register — email: %s", request.email)

    if not is_valid_password(request.password):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Password does not meet strength requirements: "
                "min 8 characters, 1 uppercase, 1 lowercase, "
                "2 digits, 1 special character."
            ),
        )

    success, message = register_user(
        request.email,
        request.name,
        request.password,
        request.employee_code,
        request.role or "USER",
    )

    if success:
        logger.info("Registration successful — email: %s", request.email)
        return {"status": "success", "message": message}

    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=message,
    )


@router.post("/auth/logout")
async def auth_logout(request: LogoutRequest):
    """Write a logout event to login_history."""
    logger.info("POST /auth/logout — user: %s", request.username)
    log_login_event(username=request.username, action="logout")
    return {"status": "success"}


@router.post("/auth/initialize-db")
async def auth_initialize_db():
    """Bootstrap all required tables. Called once from the FastAPI lifespan handler."""
    
    try:
        create_dn_diagnostics_database()
        initialize_admin_table()
        create_login_history_table()
        create_reset_tokens_table()
        create_userresponse_database()
        create_analysis_table()
        create_feedback_table()
        logger.info("DB bootstrap complete")
        return {"status": "success", "message": "All tables initialised"}
    except Exception as e:
        logger.exception("DB bootstrap failed: %s", e)
        raise HTTPException(status_code=500, detail=f"DB bootstrap failed: {e}")

@router.post("/upload-chunk")
async def upload_chunk(
    upload_id:    str        = Form(..., description="Client UUID for this upload session"),
    chunk_index:  int        = Form(..., description="0-based chunk index"),
    total_chunks: int        = Form(..., description="Total number of chunks"),
    filename:     str        = Form(..., description="Original ZIP filename"),
    chunk:        UploadFile = File(...,  description="Binary data of this chunk"),
):
    """Receive one chunk and stage it on disk."""
    data = await chunk.read()
    return save_chunk(upload_id, chunk_index, total_chunks, filename, data)


@router.post("/finalize-upload")
async def finalize_upload(
    upload_id:    str           = Form(..., description="UUID used while uploading chunks"),
    filename:     str           = Form(..., description="Original ZIP filename"),
    total_chunks: int           = Form(..., description="Total number of expected chunks"),
    mode:         Optional[str] = Form(None, description="Optional processing mode"),
):
    """Assemble all staged chunks and run the extraction pipeline."""
    return await assemble_and_process(upload_id, total_chunks, mode)


@router.delete("/cancel-upload/{upload_id}")
async def cancel_upload_endpoint(upload_id: str):
    """Delete staged chunks for an aborted upload."""
    return cancel_upload(upload_id)
