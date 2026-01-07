from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from modules.extraction import ZipExtractionService
from modules.categorization import CategorizationService
from modules.processing import ProcessingService
from modules.session import session_service
from modules.transaction_analyzer import TransactionAnalyzerService
from modules.schemas import (
    FileCategorizationResponse,
    AvailableFileTypesResponse,
    FileTypeSelectionRequest,
    CategoryCount, # Import FileCategorizationResponse explicitly
    TransactionVisualizationRequest
	
)


from modules.extraction import extract_from_directory, extract_from_zip_bytes
from modules.xml_parser_logic import parse_xml_to_dataframe
from pathlib import Path
from typing import Dict, List, Optional
import shutil
from fastapi import Body
import os
import pandas as pd
from modules.ui_journal_processor  import UIJournalProcessor, parse_ui_journal
from datetime import datetime
from collections import defaultdict
import re
import zipfile
import io
import json
import time



#  Import our central logger
from modules.logging_config import logger
from fastapi import FastAPI

import logging

logger.info("Logger initialized at startup")

app = FastAPI()


router = APIRouter()
logger.info("FastAPI app started")

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
        import traceback
        logger.error(f"Unexpected error in /debug-zip-members: {e}")  
        logger.debug(traceback.format_exc()) 
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# Simple session ID for now (use UUID in production)
CURRENT_SESSION_ID = "current_session"

# Global variable to track processed files directory (for registry endpoints)
PROCESSED_FILES_DIR = None

def set_processed_files_dir(directory: str):
    """Set the directory where processed files are stored"""
    global PROCESSED_FILES_DIR
    PROCESSED_FILES_DIR = directory
    logger.info(f"Processed files directory set to: {directory}")  

def organize_files_into_subdirectories(extract_path: Path, file_categories: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Physically move categorized files into subdirectories
    Returns updated file paths
    """
    organized_categories = {}
    
    for category, files in file_categories.items():
        # Create category subdirectory
        category_dir = extract_path / category
        category_dir.mkdir(exist_ok=True)
        
        organized_files = []
        
        for file_path_str in files:
            source = Path(file_path_str)
            if source.exists() and source.is_file():
                # Move to category subdirectory
                dest = category_dir / source.name
                try:
                    shutil.copy2(source, dest)
                    organized_files.append(str(dest))
                    logger.info(f"   Moved {source.name} to {category}/")
                except Exception as e:
                    logger.info(f"  Failed to move {source.name}: {e}")
                    continue
        
        organized_categories[category] = organized_files
    
    return organized_categories

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
    if not file.filename.endswith('.zip'):
        logger.error(" Invalid file type - only ZIP allowed")
        raise HTTPException(
            status_code=400,
            detail="Only ZIP files are accepted"
        )
    logger.info(f" Uploaded file name: {file.filename}")
    
    try:
        

        # -----------------------------------------------------------
        # â±ï¸ START TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        start_time = time.perf_counter()
        # -----------------------------------------------------------

        # ------------------ FILE READ TIMER ------------------
        t_file_start = time.perf_counter()
        logger.debug(" Reading uploaded file ")

        zip_content = await file.read()  # read only once
        t_file_end = time.perf_counter()
        logger.info(f" File read completed. Size: {len(zip_content)} bytes")
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            total_files_in_zip = len([f for f in zf.namelist() if not f.endswith('/')])
        logger.info(f"Total files in original ZIP (memory count): {total_files_in_zip}")

        logger.debug(f"FILE READ TIME: {t_file_end - t_file_start:.4f} s")

        # ------------------ ZIP EXTRACTION TIMER ------------------
        logger.info("  Extracting ZIP ...")
        t_zip_start = time.perf_counter()

        try:
            extraction_service = ZipExtractionService()
            extract_path = extraction_service.extract_zip(zip_content)
            all_files_on_disk = [p for p in Path(extract_path).rglob('*') if p.is_file()]
            total_files_on_disk = len(all_files_on_disk)
            logger.info(f"Total files in extracted directory (including nested ZIPs if extracted later): {total_files_on_disk}")
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
        logger.info(" STEP 3: Checking for nested ZIP files.")
        t_nested_zip_start = time.perf_counter()

        nested_zip_files = [p for p in Path(extract_path).rglob('*.zip')]
        logger.info(f"Found nested ZIPs: {len(nested_zip_files)}")


        for nested_zip_path in nested_zip_files:
                logger.info(f" Handling nested ZIP: {nested_zip_path.relative_to(extract_path)}")
            
                try:
                    with open(nested_zip_path, 'rb') as f:
                        nested_content = f.read()
                    # Extract nested zip into a directory with the same name
                    
                    nested_extract_path = extraction_service.extract_zip(nested_content)
                    all_files_after_nested = [p for p in Path(extract_path).rglob('*') if p.is_file()]
                    logger.info(f"   Nested ZIP extracted to {nested_extract_path}")
                    logger.info(f"Grand total files after nested ZIP extraction: {len(all_files_after_nested)}")
                except Exception as e:
                    logger.error(f"  Failed to extract nested ZIP {nested_zip_path.name}: {e}", exc_info=True)
                    
                
        t_nested_zip_end = time.perf_counter()
        logger.info(f"NESTED ZIP EXTRACTION TIME: {t_nested_zip_end - t_nested_zip_start:.4f} s")

        # ------------------ CATEGORIZATION + ACU EXTRACTION (COMBINED) ------------------
        # Extract ACU files first, then include in categorization result
        logger.info(f" Extracting ACU XML files (jdd*, x3*) directly from ZIP...")
        acu_logs = []
        t_cat_start = time.perf_counter()
        
        #  Extract ACU files from memory first to get their names
        try:
            acu_files = extract_from_zip_bytes(zip_content, acu_logs, target_prefixes=('jdd', 'x3'))
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
        
        # Step 2: Categorize files from the extracted directory, excluding ACU files
        # Initialize the categories dictionary here
        file_categories = {
            'customer_journals': [], 'ui_journals': [], 'trc_trace': [],
            'trc_error': [], 'registry_files': [], 'acu_files': [], 'unidentified': []
        }
        
        # Step 3: Add the correctly identified ACU files to the categories FIRST
        if acu_files:
            file_categories['acu_files'] = list(acu_files.keys())
            logger.info(f" Added {len(acu_files)} ACU files to final categories.")
        
        # Step 4: Run on-disk categorization, which will populate the SAME dictionary
        logger.info("Running on-disk categorization ...")
        categorization_service = CategorizationService()
        categorization_service.categorize_files(extract_path, file_categories, acu_filenames_to_exclude, mode=mode)
        
        t_cat_end = time.perf_counter()
        logger.debug(f"CATEGORIZATION + ACU EXTRACTION TIME: {t_cat_end - t_cat_start:.4f} s")

        #--------------------------------------------
        registry_contents = {}
        for reg_file in file_categories.get('registry_files', []):
            try:
                if Path(reg_file).exists():
                    with open(reg_file, 'rb') as f:
                        registry_contents[Path(reg_file).name] = f.read()
                        logger.info(f" Loaded registry file into memory: {Path(reg_file).name}")
            except Exception as e:
                logger.error(f"Failed to read registry file {reg_file}: {e}")
                #---------------------------------------------------------------
        
        # ------------------ SESSION CREATION TIMER ------------------
        logger.info("Creating/updating session")
        t_sess_start = time.perf_counter()
        set_processed_files_dir(str(extract_path))
        session_service.create_session(CURRENT_SESSION_ID, file_categories, extract_path)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extracted_files', acu_files)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extraction_logs', acu_logs)
        session_service.update_session(CURRENT_SESSION_ID, 'registry_contents', registry_contents)
        t_sess_end = time.perf_counter()
        logger.debug(f"SESSION SAVE TIME: {t_sess_end - t_sess_start:.4f} s")

        # ------------------ PROCESSING TIMER ------------------
        logger.info(" prepare response")
        t_proc_start = time.perf_counter()
        processing_service = ProcessingService()
        result = processing_service.prepare_response(file_categories, extract_path)
        result.acu_extraction_logs = acu_logs
        t_proc_end = time.perf_counter()
        logger.debug(f"PROCESSING TIME: {t_proc_end - t_proc_start:.4f} s")


        # -----------------------------------------------------------
        # ⏱️ END TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        end_time = time.perf_counter()
        logger.info(f"TOTAL ZIP PROCESSING TIME: {end_time - start_time:.4f} s")
        # -----------------------------------------------------------

        return result
    except HTTPException:
        raise   
    except Exception as e:
        import traceback
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
        if not file.filename.endswith('.zip'):
            logger.error(f"Invalid file type uploaded: {file.filename}")  
            raise HTTPException(
                status_code=400,
                detail="Only ZIP files are accepted"
            )

        zip_content = await file.read()
        logger.debug(f"Read {len(zip_content)} bytes from uploaded file: {file.filename}")  

        acu_logs = []
        acu_files = extract_from_zip_bytes(zip_content, acu_logs, target_prefixes=('jdd', 'x3'))
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

    except Exception as e:
        import traceback
        logger.error(f"Unexpected error during ACU extraction for file {file.filename}: {e}")  
        logger.debug(traceback.format_exc())  
        raise HTTPException(
            status_code=500,
            detail=f"Error extracting files: {str(e)}\n{traceback.format_exc()}"
        )

@router.get("/get-registry-contents")
async def get_registry_contents(session_id: str = Query(default=CURRENT_SESSION_ID)):
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

    try:
        if not session_service.session_exists(session_id):
            raise HTTPException(status_code=404, detail="No session found")
        
        session_data = session_service.get_session(session_id)
        registry_contents = session_data.get('registry_contents', {})
        
        # Convert bytes to base64 for JSON serialization
        import base64
        encoded_contents = {}
        for filename, content_bytes in registry_contents.items():
            if isinstance(content_bytes, bytes):
                encoded_contents[filename] = base64.b64encode(content_bytes).decode('utf-8')
            else:
                encoded_contents[filename] = content_bytes
        
        logger.info(f"Serving {len(encoded_contents)} registry files from session")
        
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

@router.get("/get-acu-files")
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

@router.post("/parse-acu-files")
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
async def get_available_file_types(session_id: str = Query(default=CURRENT_SESSION_ID)):
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
async def select_file_type(request: FileTypeSelectionRequest,session_id: str = Query(default=CURRENT_SESSION_ID)):
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
async def analyze_customer_journals(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    FUNCTION:
        analyze_customer_journals

    DESCRIPTION:
        Analyzes customer journal files from a processed ZIP session and extracts transaction data.
        Combines transactions from all customer journal files, calculates statistics per transaction type,
        and updates the session with extracted transaction data and source file details.

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
                'source_file_count': int          # Number of source files processed
            }

    RAISES:
        HTTPException :
            - 404 if the session does not exist
            - 400 if no customer journal files are found
            - 400 if no transactions could be extracted
            - 500 for unexpected errors during processing
    """
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
        
        if not journal_files:
            logger.error("No customer journal files found")
            raise HTTPException(
                status_code=400,
                detail="No customer journal files found in the uploaded package."
            )
        
        logger.info(f" Found {len(journal_files)} customer journal file(s)")
        
        # Initialize analyzer
        analyzer = TransactionAnalyzerService()
        
        all_transactions_df = []
        source_files = []
        source_file_map = {}
        
        for journal_file in journal_files:
            logger.info(f"   Processing: {journal_file}")
            logger.info(f"   Path object: {Path(journal_file)}")
            logger.info(f"   Filename (name): {Path(journal_file).name}")
            logger.info(f"   Filename (stem): {Path(journal_file).stem}")
            
            source_filename = Path(journal_file).stem
            source_files.append(source_filename)
            
            try:
                df = analyzer.parse_customer_journal(journal_file)
                
                if df is None or df.empty:
                    logger.debug(f"No transactions found in {source_filename}")
                    continue
                
                logger.info(f"Found {len(df)} transactions")
                
                all_transactions_df.append(df)
                
                if 'Transaction ID' in df.columns:
                    file_transactions_ids = df['Transaction ID'].tolist()
                    source_file_map[source_filename] = file_transactions_ids
                
            except Exception as e:
                logger.error(f"Error processing {journal_file}: {str(e)}")
                import traceback
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

        # ADD THESE DEBUG LINES
        logger.info(f"\n CONVERTING TO RECORDS:")
        logger.info(f"   Total records: {len(transaction_records)}")
        if transaction_records:
            sample = transaction_records[0]
            logger.info(f"   Sample record keys: {list(sample.keys())}")
            logger.info(f"   Sample 'Source File' value: '{sample.get('Source File', 'KEY NOT FOUND')}'")
        
        # Store in session (remove duplicates from source_files)
        unique_source_files = list(set(source_files))
        unique_source_files.sort()

        # DEBUG: Print what we're about to store
        logger.info(f" Unique source files being stored: {unique_source_files}")
        logger.info(f" Total source files count: {len(unique_source_files)}")

        session_service.update_session(session_id, 'transaction_data', transaction_records)
        session_service.update_session(session_id, 'source_files', unique_source_files)
        session_service.update_session(session_id, 'source_file_map', source_file_map)
        
        stats = []
        for txn_type in combined_df['Transaction Type'].unique():
            type_df = combined_df[combined_df['Transaction Type'] == txn_type]
            successful = len(type_df[type_df['End State'] == 'Successful'])
            unsuccessful = len(type_df[type_df['End State'] == 'Unsuccessful'])
            total = len(type_df)
            
            stats.append({
                'Transaction Type': txn_type,
                'Total': total,
                'Successful': successful,
                'Unsuccessful': unsuccessful,
                'Success Rate': f"{(successful/total*100):.1f}%" if total > 0 else "0%"
            })
        
        logger.info("Customer journal analysis completed successfully")
        
        return {
            'message': 'Customer journals analyzed successfully',
            'total_transactions': len(combined_df),
            'statistics': stats,
            'source_files': source_files,
            'source_file_count': len(source_files)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )
@router.get("/get-transactions-with-sources")
async def get_transactions_with_sources(session_id: str = Query(default=CURRENT_SESSION_ID)):
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
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving transactions: {str(e)}"
        )


@router.post("/filter-transactions-by-sources")
async def filter_transactions_by_sources(source_files: List[str] = Body(..., embed=True),session_id: str = Query(default=CURRENT_SESSION_ID)):
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
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error filtering transactions: {str(e)}"
        )

@router.get("/transaction-statistics")
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
                'Total': total,
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
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error generating statistics: {str(e)}"
        )

@router.post("/compare-transactions-flow")
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

        # Get file categories from session
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])

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

                    # Try to find matching UI journal first
                    matching_ui_journal = None
                    for ui_journal in ui_journals:
                        ui_journal_name = Path(ui_journal).stem
                        if ui_journal_name == txn_source_file:
                            matching_ui_journal = ui_journal
                            logger.info(f" Found matching UI journal for {txn_label}: {ui_journal_name}")
                            break

                    # If no exact match, try all UI journals
                    ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals

                    for ui_journal_path in ui_journals_to_check:
                        logger.info(f" Parsing UI journal for {txn_label}: {ui_journal_path}")

                        ui_df = parse_ui_journal(ui_journal_path)

                        if not ui_df.empty:
                            logger.info(f" Parsed {len(ui_df)} UI events for {txn_label}")

                            processor = UIJournalProcessor(ui_journal_path)
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
                                                                from datetime import date
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
async def get_current_selection(session_id: str = Query(default=CURRENT_SESSION_ID)):
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
async def debug_session(session_id: str = Query(default=CURRENT_SESSION_ID)):
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


@router.post("/visualize-individual-transaction-flow")
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

        # Get file categories
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        logger.info(f" Found {len(ui_journals)} UI journal file(s)")

        if ui_journals:
            try:
                txn_source_file = str(txn_data.get('Source File', ''))
                logger.info(f" Transaction source file: {txn_source_file}")

                matching_ui_journal = None
                for ui_journal in ui_journals:
                    ui_journal_name = Path(ui_journal).stem
                    if ui_journal_name == txn_source_file:
                        matching_ui_journal = ui_journal
                        logger.info(f" Found matching UI journal: {ui_journal_name}")
                        break

                ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals

                for ui_journal_path in ui_journals_to_check:
                    logger.debug(f" Parsing UI journal: {ui_journal_path}")
                    ui_df = parse_ui_journal(ui_journal_path)

                    if not ui_df.empty:
                        logger.info(f" Parsed {len(ui_df)} UI events")
                        processor = UIJournalProcessor(ui_journal_path)
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
                                    print(" No screens found in time range")
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
                                    
                                    # Build detailed flow for unique screens
                                    ui_flow_details = []
                                    
                                    for i, screen_name in enumerate(unique_screens):
                                        info = screen_info.get(screen_name)
                                        
                                        if not info:
                                            ui_flow_details.append({
                                                'screen': screen_name,
                                                'timestamp': '',
                                                'duration': None
                                            })
                                            continue
                                        
                                        first_time = info['first_time']
                                        
                                        # Calculate duration: from first occurrence of THIS screen
                                        # to first occurrence of NEXT screen
                                        duration = None
                                        if i < len(unique_screens) - 1:
                                            next_screen = unique_screens[i + 1]
                                            next_info = screen_info.get(next_screen)
                                            
                                            if next_info and next_info['first_time']:
                                                try:
                                                    from datetime import date
                                                    dt1 = datetime.combine(date.today(), first_time)
                                                    dt2 = datetime.combine(date.today(), next_info['first_time'])
                                                    duration = (dt2 - dt1).total_seconds()
                                                except Exception as e:
                                                    duration = None
                                        
                                        ui_flow_details.append({
                                            'screen': screen_name,
                                            'timestamp': str(first_time) if first_time else '',
                                            'duration': duration
                                        })
                                    
                                    if ui_flow_details and len(ui_flow_details) > 0:
                                        ui_flow_screens = ui_flow_details
                                        has_flow = True
                                        
                                        with_duration = sum(1 for s in ui_flow_details if s['duration'] is not None)
                                        # print(f" Created detailed flow: {len(ui_flow_details)} unique screens, {with_duration} with durations")
                                        
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
                                # print(f" Enhancement failed: {e}")
                                import traceback
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

@router.post("/generate-consolidated-flow")
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

    try:
        logger.info(f" Starting consolidated flow generation for type '{transaction_type}' from source '{source_file}'")
        logger.debug(f"Session ID received: {session_id}")
        
        print(f" Generating consolidated flow for {transaction_type} from {source_file}")
        
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
        
        logger.info(f" {len(filtered_df)} transactions matched filters")
        print(f" Found {len(filtered_df)} transactions")
        
        # Get UI journals
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        
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
        print(f" Found matching UI journal: {matching_ui_journal}")
        
        # Parse UI journal
        logger.info("Parsing UI journal file")
        ui_df = parse_ui_journal(matching_ui_journal)
        
        if ui_df.empty:
            logger.error("Parsed UI journal is empty")
            raise HTTPException(
                status_code=400,
                detail="UI journal is empty or could not be parsed"
            )
        
        logger.info(f"UI journal parsed successfully with {len(ui_df)} events")
        print(f" Parsed UI journal with {len(ui_df)} events")
        
        # Create processor
        logger.debug("Creating UIJournalProcessor instance")
        processor = UIJournalProcessor(matching_ui_journal)
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
            
            logger.debug(f"Times parsed â†’ Start: {start_time}, End: {end_time}")
            
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
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate consolidated flow: {str(e)}"
        )
    
from pydantic import BaseModel

# Add this class near the top of routes.py with other models
class TransactionAnalysisRequest(BaseModel):
    transaction_id: str

# Then replace the endpoint:
@router.post("/analyze-transaction-llm")
async def analyze_transaction_llm(request: TransactionAnalysisRequest,session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    FUNCTION:
        generate_consolidated_flow

    DESCRIPTION:
        Generates a consolidated UI flow visualization for **all transactions**
        of a specific transaction type originating from a given source file.
        The function processes transaction data, matches the correct UI journal,
        extracts screen flows for each transaction, and returns aggregated
        results including screens, transitions, and per-transaction flows.

    USAGE:
        result = generate_consolidated_flow(
            source_file="ATM1",
            transaction_type="Cash Withdrawal",
            session_id="12345"
        )

    PARAMETERS:
        source_file (str):
            Name of the source UI journal file (without extension).
            Used to match UI logs against transaction data.

        transaction_type (str):
            Transaction type to filter on (e.g., "Cash Withdrawal",
            "Balance Inquiry", etc.).

        session_id (str):
            Session identifier used to retrieve processed ZIP data from
            session storage. Defaults to the current session.

    RETURNS:
        dict:
            A dictionary containing the consolidated UI flow analysis:
                {
                    "source_file": str,
                    "transaction_type": str,
                    "total_transactions": int,
                    "transactions_with_flow": int,
                    "successful_count": int,
                    "unsuccessful_count": int,
                    "screens": list[str],                     # unique screens
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
            - No matching transactions found
            - No matching UI journal found

        HTTPException 400:
            - Missing or empty UI journal
            - Missing transaction data in session

        HTTPException 500:
            - Unexpected errors during flow extraction or processing
    """
    try:
        transaction_id = request.transaction_id
        logger.info(f" Analyzing transaction with LLM: {transaction_id}")
        logger.debug(f"Request data: {request.dict()}")
        
        # Check session
        if not session_service.session_exists(session_id):
            logger.error(f"No session found for session_id: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found"
            )
        
        session_data = session_service.get_session(session_id)
        logger.debug(f"Session data retrieved for session_id {session_id}")
        
        # Get transaction data
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            logger.error(f"No transaction data available for session_id: {session_id}")
            raise HTTPException(
                status_code=400,
                detail="No transaction data available"
            )
        
        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)
        
        # Find the transaction
        if transaction_id not in df['Transaction ID'].values:
            logger.error(f"Transaction {transaction_id} not found in session {session_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found"
            )
        
        txn_data = df[df['Transaction ID'] == transaction_id].iloc[0]
        transaction_log = str(txn_data.get('Transaction Log', ''))
        
        if not transaction_log:
            logger.error(f"No transaction log available for transaction {transaction_id}")
            raise HTTPException(
                status_code=400,
                detail="No transaction log available for this transaction"
            )
        
        logger.info(f" Found transaction log ({len(transaction_log)} characters)")

        # Call LLM for analysis
        try:
            import ollama
            
            messages = [
                {
                    "role": "system", 
                    "content": "You are a log analysis expert specializing in ATM transaction diagnostics. Analyze the provided transaction log for anomalies, errors, and potential issues. Provide a clear, concise analysis in plain text format - do not use JSON in your response. Focus on: 1) What happened, 2) Why it might have happened, 3) Potential root causes."
                },
                {
                    "role": "user", 
                    "content": f"Analyze this ATM transaction log for anomalies and issues:\n\n{transaction_log}"
                }
            ]
            
            logger.info(" Calling Ollama model...")
            logger.debug(f"LLM messages payload: {messages}")
            
            response = ollama.chat(model="llama3_log_analyzer", messages=messages)
            raw_response = response['message']['content'].strip()
            logger.info(f" LLM analysis complete ({len(raw_response)} characters)")
            
            # Structure the response
            structured_response = {
                "summary": "Transaction log analysis completed",
                "analysis": raw_response,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "metadata": {
                    "transaction_id": transaction_id,
                    "model": "llama3_log_analyzer",
                    "log_length": len(transaction_log),
                    "response_length": len(raw_response),
                    "analysis_type": "anomaly_detection",
                    "transaction_type": str(txn_data.get('Transaction Type', 'Unknown')),
                    "transaction_state": str(txn_data.get('End State', 'Unknown')),
                    "start_time": str(txn_data.get('Start Time', '')),
                    "end_time": str(txn_data.get('End Time', '')),
                    "source_file": str(txn_data.get('Source File', 'Unknown'))
                }
            }
            
            logger.debug(f"Structured response prepared for transaction {transaction_id}")
            return structured_response
            
        except ImportError:
            logger.error("Ollama is not installed")
            raise HTTPException(
                status_code=500,
                detail="Ollama is not installed. Please install it with: pip install ollama"
            )
        except Exception as e:
            logger.exception(f"LLM analysis error for transaction {transaction_id}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"LLM analysis failed: {str(e)}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Analysis failed for transaction {transaction_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )
    
# Add this Pydantic model near the top with other models
class FeedbackSubmission(BaseModel):
    transaction_id: str
    rating: int
    alternative_cause: str
    comment: str
    user_name: str
    user_email: str
    model_version: str
    original_llm_response: str

@router.post("/submit-llm-feedback")
async def submit_llm_feedback(feedback: FeedbackSubmission,session_id: str = Query(default=CURRENT_SESSION_ID)):
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

RETURNS:
    dict:
        {
            "status": "success",
            "message": "Thank you <name>! Your feedback has been recorded.",
            "timestamp": "<YYYY-MM-DD HH:MM:SS>"
        }

        - `status`: Indicates whether the feedback submission was successful.
        - `message`: User-friendly confirmation message.
        - `timestamp`: Server-generated timestamp of the feedback record.

SIDE EFFECTS:
    - Appends feedback as a JSON line in `llm_feedback.json`
    - Stores feedback in session under `feedback_data` list
    - Creates session if missing

RAISES:
    HTTPException 500:
        - Raised when unexpected failures occur while processing or storing
          the feedback (e.g., file write errors, session update failures).
"""

    try:
        logger.info(f" Submitting feedback for transaction: {feedback.transaction_id}")
        logger.debug(f"Feedback payload: {feedback.dict()}")
        
        # Create feedback record
        feedback_record = {
            "transaction_id": feedback.transaction_id,
            "rating": feedback.rating,
            "alternative_cause": feedback.alternative_cause,
            "comment": feedback.comment,
            "user_name": feedback.user_name,
            "user_email": feedback.user_email,
            "model_version": feedback.model_version,
            "original_llm_response": feedback.original_llm_response,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "submission_date": datetime.now().strftime("%Y-%m-%d"),
            "submission_time": datetime.now().strftime("%H:%M:%S"),
            "session_id": session_id
        }
        
        # Save to file (append mode)
        feedback_file = Path("llm_feedback.json")
        
        try:
            with open(feedback_file, "a") as f:
                f.write(json.dumps(feedback_record) + "\n")
            logger.info(f" Feedback saved to file: {feedback_file}")
        except Exception as e:
            logger.error(f" Could not save feedback to file {feedback_file}: {str(e)}")
        
        # Also store in session for immediate retrieval
        if not session_service.session_exists(session_id):
            session_service.create_session(session_id)
            logger.debug(f"Created new session: {session_id}")
        
        session_data = session_service.get_session(session_id)
        
        if 'feedback_data' not in session_data:
            session_data['feedback_data'] = []
        
        session_data['feedback_data'].append(feedback_record)
        session_service.update_session(session_id, 'feedback_data', session_data['feedback_data'])
        logger.info(f" Feedback stored in session for session_id: {session_id}")
        
        return {
            "status": "success",
            "message": f"Thank you {feedback.user_name}! Your feedback has been recorded.",
            "timestamp": feedback_record['timestamp']
        }
        
    except Exception as e:
        logger.exception(f"Failed to submit feedback for transaction {feedback.transaction_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit feedback: {str(e)}"
        )



@router.get("/get-feedback/{transaction_id}")
async def get_feedback(transaction_id: str,session_id: str = Query(default=CURRENT_SESSION_ID)):
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
    
class CounterDataRequest(BaseModel):
    transaction_id: str
    source_file: str

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
        FUNCTION: parse_counter_data_from_trc

        DESCRIPTION:
            Parses counter data from TRC trace log lines using intelligent field detection. 
            Handles missing or optional fields by detecting patterns for UnitName, currency, 
            and numeric values. Returns a list of structured counter records.

        USAGE:
            counters = parse_counter_data_from_trc(log_lines)

        PARAMETERS:
            log_lines (list) : List of strings, each representing a line from a TRC trace file.

        RETURNS:
            list : A list of dictionaries, each representing a counter record with fields:
                - 'No'        : Counter number
                - 'Ty'        : Counter type
                - 'ID'        : Counter ID
                - 'UnitName'  : Unit name (may be empty if missing)
                - 'Cur'       : Currency code (3-letter, may be empty)
                - 'Val'       : Value
                - 'Ini'       : Initial count
                - 'Cnt'       : Current count
                - 'RCnt'      : Reject count
                - 'Safe'      : Safe count
                - 'Min'       : Minimum value
                - 'Max'       : Maximum value
                - 'Disp'      : Display field (empty by default)
                - 'Pres'      : Present field (empty by default)
                - 'Retr'      : Retracted field (empty by default)
                - 'A'         : AppL field
                - 'DevL'      : DevL field
                - 'St'        : Status field
                - 'HWsens'    : Hardware sensor field
                - 'Record_Type': Always 'Logical'

        RAISES:
            None : Function handles parsing errors internally and skips invalid lines.
"""

    import re
    counter_rows = []
    
    # Find header line
    header_line = None
    header_idx = -1
    
    for idx, line in enumerate(log_lines):
        if 'No' in line and 'Ty' in line and 'UnitName' in line:
            header_line = line
            header_idx = idx
            break
    
    if not header_line or header_idx == -1:
        return []
    
    # Parse data lines
    for idx in range(header_idx + 1, len(log_lines)):
        line = log_lines[idx]
        
        # Skip empty, CCdm, or separator lines
        if (not line.strip() or 
            'CCdm' in line or 
            'usTellerID' in line or
            line.strip().startswith('*')):
            continue
        
        # Skip continuation lines (start with whitespace)
        if line.startswith(' ') or line.startswith('\t'):
            continue
        
        # Validate this is a data line (starts with digit)
        if not line[0].isdigit():
            continue
        
        try:
            counter_data = {}
            
            # Parse first 3 fields using single space (always present)
            first_part = line[:12].strip()
            first_fields = first_part.split()
            
            if len(first_fields) < 3:
                continue
            
            counter_data['No'] = first_fields[0]
            counter_data['Ty'] = first_fields[1]
            counter_data['ID'] = first_fields[2]
            
            # Parse remaining part (split by 2+ spaces)
            remaining_part = line[12:]
            remaining_fields = re.split(r'  +', remaining_part.strip())
            
            if not remaining_fields:
                continue
            
            field_idx = 0
            
            # Intelligently detect UnitName and Cur
            # UnitName: alphanumeric with dots/underscores (SLOT1, HEADUNIT.RET)
            # Cur: exactly 3 uppercase letters (USD, EUR, INR)
            
            first_field = remaining_fields[field_idx] if field_idx < len(remaining_fields) else ''
            
            # Check if first field is a currency code
            is_currency = (len(first_field) == 3 and 
                          first_field.isalpha() and 
                          first_field.isupper())
            
            if is_currency:
                # No UnitName present, first field is Cur
                counter_data['UnitName'] = ''
                counter_data['Cur'] = first_field
                field_idx += 1
            else:
                # First field is UnitName
                counter_data['UnitName'] = first_field
                field_idx += 1
                
                # Check if next field is currency
                if field_idx < len(remaining_fields):
                    next_field = remaining_fields[field_idx]
                    if (len(next_field) == 3 and 
                        next_field.isalpha() and 
                        next_field.isupper()):
                        counter_data['Cur'] = next_field
                        field_idx += 1
                    else:
                        counter_data['Cur'] = ''
                else:
                    counter_data['Cur'] = ''
            
            # Parse numeric fields: Val, Init, Actn, Rej, Safe, Min, Max
            numeric_field_names = ['Val', 'Ini', 'Cnt', 'RCnt', 'Safe', 'Min', 'Max']
            
            for field_name in numeric_field_names:
                if field_idx < len(remaining_fields):
                    value = remaining_fields[field_idx]
                    # Check if it's a numeric field
                    if value.replace('-', '').isdigit():
                        counter_data[field_name] = value
                        field_idx += 1
                    else:
                        # Stop consuming numeric fields if we hit a non-numeric
                        counter_data[field_name] = ''
                        break
                else:
                    counter_data[field_name] = ''
            
            # Set empty fields
            counter_data['Disp'] = ''
            counter_data['Pres'] = ''
            counter_data['Retr'] = ''
            
            # Parse remaining fields: AppL (A), DevL, Status (St), HWsens
            # These are typically: FALSE FALSE 0/OK
            counter_data['A'] = remaining_fields[field_idx] if field_idx < len(remaining_fields) else ''
            field_idx += 1
            
            counter_data['DevL'] = remaining_fields[field_idx] if field_idx < len(remaining_fields) else ''
            field_idx += 1
            
            counter_data['St'] = remaining_fields[field_idx] if field_idx < len(remaining_fields) else ''
            field_idx += 1
            
            counter_data['HWsens'] = remaining_fields[field_idx] if field_idx < len(remaining_fields) else ''
            
            counter_data['Record_Type'] = 'Logical'
            counter_rows.append(counter_data)
            
        except Exception as e:
            continue
    
    return counter_rows

def parse_time_from_trc(time_str: str) -> datetime.time:
    """Parse time from TRC trace format (HH:MM:SS or HH:MM:SS.MS)"""
    try:
        if '.' in time_str:
            time_str = time_str.split('.')[0]
        return datetime.strptime(time_str, '%H:%M:%S').time()
    except Exception as e:
        return None

def extract_counter_blocks(trc_file_path: str) -> list:
    """
        FUNCTION: extract_counter_blocks

        DESCRIPTION:
            Extracts all counter blocks from a given TRC trace file.
            Each 'CCdmCashUnitInfoDataEx' block is parsed separately and returned
            as a dictionary containing the block's timestamp, time object, and counter data.
            CRITICAL: Each block is kept separate; no merging of blocks occurs.

        USAGE:
            counter_blocks = extract_counter_blocks("path/to/trc_file.trc")

        PARAMETERS:
            trc_file_path (str) : Path to the TRC trace file to be processed.

        RETURNS:
            list : A list of dictionaries, each representing a counter block with:
                - 'time' (datetime.time)        : Time extracted from the TRC block
                - 'timestamp' (str)             : Timestamp string (HH:MM:SS.ss) from the TRC block
                - 'data' (list of dicts)        : Counter data extracted using parse_counter_data_from_trc

        RAISES:
            Exception : Any error during file reading or parsing is caught and printed,
                        but the function will return an empty list if critical errors occur.
"""

    all_counter_blocks = []
    
    try:
        with open(trc_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            lines = content.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Look for counter block marker
            if 'CCdmCashUnitInfoDataEx' in line:
                # Extract timestamp from THIS line or previous line
                timestamp_str = None
                block_time = None
                
                # Check current line for timestamp (format: XXXXX YYMMDD HH:MM:SS.ss)
                ts_match = re.search(r'(\d+)\s+(\d{6})\s+(\d{2}:\d{2}:\d{2}\.\d{2})', line)
                if not ts_match and i > 0:
                    # Check previous line
                    ts_match = re.search(r'(\d+)\s+(\d{6})\s+(\d{2}:\d{2}:\d{2}\.\d{2})', lines[i-1])
                
                if ts_match:
                    timestamp_str = ts_match.group(3)  # HH:MM:SS.ss
                    try:
                        block_time = datetime.strptime(timestamp_str, '%H:%M:%S.%f').time()
                    except:
                        pass
                
                # Extract counter data lines for THIS block only
                block_lines = []
                i += 1
                
                # Collect lines until we hit another CCdmCashUnitInfoDataEx or empty line pattern
                while i < len(lines):
                    current_line = lines[i]
                    
                    # Stop if we hit another counter block
                    if 'CCdmCashUnitInfoDataEx' in current_line:
                        i -= 1  # Back up so we process this block next iteration
                        break
                    
                    # Stop if we hit another timestamp line (new trace entry)
                    if re.search(r'^\d+\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{2}', current_line):
                        break
                    
                    # Add line to current block
                    block_lines.append(current_line)
                    i += 1
                
                # Parse the counter data from this block
                counter_data = parse_counter_data_from_trc(block_lines)
                
                # CRITICAL: Add as NEW BLOCK - NEVER MERGE
                if counter_data and timestamp_str:
                    all_counter_blocks.append({
                        'time': block_time,
                        'timestamp': timestamp_str,
                        'data': counter_data
                    })
            
            i += 1
    
    except Exception as e:
        print(f"Error extracting counter blocks: {e}")
        import traceback
        traceback.print_exc()
    
    return all_counter_blocks
    
@router.get("/get-matching-sources-for-trc")
async def get_matching_sources_for_trc(session_id: str = Query(default=CURRENT_SESSION_ID)):
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

    try:
        if not session_service.session_exists(session_id):
            raise HTTPException(status_code=404, detail="No session found")
        
        session_data = session_service.get_session(session_id)
        
        # Get all source files
        all_sources = session_data.get('source_files', [])
        
        # Get TRC trace files
        file_categories = session_data.get('file_categories', {})
        trc_trace_files = file_categories.get('trc_trace', [])
        
        if not trc_trace_files:
            return {"matching_sources": []}
        
        matching_sources = []
        
        for source in all_sources:
            # Convert YYYYMMDD to YYMMDD
            source_date_short = source[2:] if len(source) == 8 else source
            
            # Check if any TRC file contains this date
            for trc_file in trc_trace_files:
                try:
                    with open(trc_file, 'r', encoding='utf-8', errors='ignore') as f:
                        first_lines = ''.join([f.readline() for _ in range(100)])
                    
                    if source_date_short in first_lines:
                        matching_sources.append(source)
                        break
                except:
                    continue
        
        logger.info(f" Found {len(matching_sources)} sources with matching TRC trace files")
        
        return {"matching_sources": matching_sources}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.post("/get-counter-data")
async def get_counter_data(
    request: CounterDataRequest,
    session_id: str = Query(default=CURRENT_SESSION_ID)
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
                - "first_counter" (dict)           : Counter at or after transaction start (dynamic)
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
        
        # Get TRC trace files
        file_categories = session_data.get('file_categories', {})
        trc_trace_files = file_categories.get('trc_trace', [])
        
        if not trc_trace_files:
            raise HTTPException(
                status_code=400,
                detail="No TRC trace files available"
            )
        
        # print(f" Found {len(trc_trace_files)} TRC trace file(s)")
        
        # Parse transaction date from source file (format: YYYYMMDD -> YYMMDD)
        txn_date_full = request.source_file  # e.g., "20250404"
        txn_date_short = txn_date_full[2:] if len(txn_date_full) == 8 else txn_date_full  # "250404"
        
        # print(f" Transaction date: {txn_date_full} (searching for {txn_date_short})")
        
        # Find matching TRC trace file by parsing its content for the date
        matching_trc = None
        
        for trc_file in trc_trace_files:
            try:
                # Read first few lines to check date
                with open(trc_file, 'r', encoding='utf-8', errors='ignore') as f:
                    first_lines = ''.join([f.readline() for _ in range(100)])
                    
                # Check if this TRC file contains the transaction date (YYMMDD format)
                if txn_date_short in first_lines:
                    matching_trc = trc_file
                    # print(f" Found matching TRC trace: {Path(trc_file).name}")
                    break
            except Exception as e:
                # print(f" Error reading {trc_file}: {e}")
                continue
        
        if not matching_trc:
            raise HTTPException(
                status_code=404,
                detail=f"No matching TRC trace file found for date '{txn_date_full}' (searched for '{txn_date_short}')"
            )
        
        # print(f" Found matching TRC trace: {matching_trc}")
        
        # Extract counter blocks from TRC trace file
        txn_start_time = str(txn_data.get('Start Time', ''))
        txn_end_time = str(txn_data.get('End Time', ''))

        # print(f"Transaction times: {txn_start_time} to {txn_end_time}")

        # OPTIMIZATION: Extract ALL counter blocks from TRC file ONCE
        all_counter_blocks = extract_counter_blocks(matching_trc)

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
            from datetime import datetime, time as dt_time
            
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
            
            # 1. Start counter: STATIC - absolute first block in the file
            start_block = all_counter_blocks[0]
            start_counter_data = start_block['data']
            start_timestamp = start_block['timestamp']
            
            # 2. First counter: DYNAMIC - find counter at or just AFTER transaction start time
            first_block = None
            
            if txn_start_dt:
                for i, block in enumerate(all_counter_blocks):
                    block_time = block.get('time')
                    if block_time:
                        # print(f"  Block {i}: time={block_time}, comparing with txn_start={txn_start_dt}")
                        if block_time >= txn_start_dt:
                            first_block = block
                            # print(f"   Found first counter at/after transaction start: {block_time}")
                            break
            
            # Fallback to first block if no counter found after start time
            if not first_block:
                # print(f"   No counter found at/after transaction start, using first block")
                first_block = all_counter_blocks[0]
            
            first_counter_data = first_block['data']
            first_timestamp = first_block['timestamp']
            
            # 3. Last counter: STATIC - absolute last block in the file
            last_block = all_counter_blocks[-1]
            last_counter_data = last_block['data']
            last_timestamp = last_block['timestamp']
            
            # print(f" Start counter (static - first in file): {len(start_counter_data)} rows at {start_timestamp}")
            # print(f" First counter (dynamic - at/after txn start): {len(first_counter_data)} rows at {first_timestamp}")
            # print(f" Last counter (static - last in file): {len(last_counter_data)} rows at {last_timestamp}")
        
        # Get transaction date
        txn_date = txn_date_full

        # Format the date for display (YYYYMMDD -> "DD Month YYYY")
        txn_date_formatted = txn_date
        if len(txn_date) == 8:  # YYYYMMDD
            try:
                from datetime import datetime
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
            transactions_subset['Transaction Type'].isin(['CIN/CI', 'COUT/GA'])
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
                    from datetime import datetime
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
                
                if txn_type == 'COUT/GA':
                    # COUT pattern: "Dispense info - 1 note(s) of 500,00 INR from cassette 5 (SLOT3)"
                    for log_line in txn_log.split('\n'):
                        match = re.search(r'(\d+)\s+note\(s\)\s+of\s+([\d,\.]+)\s+([A-Z]{3})', log_line, re.IGNORECASE)
                        if match:
                            note_count = match.group(1)
                            amount = match.group(2).replace(',', '.')  # Handle comma as decimal separator
                            currency = match.group(3)
                            count_info.append(f"{currency} {amount} x{note_count}")
                
                elif txn_type == 'CIN/CI':
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
        
        # Find this section in get_counter_data endpoint (around line 1890):
        response_data = {
            "transaction_id": request.transaction_id,
            "source_file": request.source_file,
            "all_blocks": all_counter_blocks,
            "column_descriptions": get_counter_column_descriptions(),  # ADD THIS LINE
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
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get counter data: {str(e)}"
        )
    
def get_counter_column_descriptions():
    """Return descriptions for counter table columns"""
    return {
        'No': 'Cassette number',
        'Ty': 'Type',
        'ID': 'Unit ID',
        'UnitName': 'UnitName',
        'Cur': 'Currency',
        'Val': 'Denomination',
        'Ini': 'Ini - count in number',
        'Cnt': 'Cnt - Remaining counters formula: INI - (RETRACT + DISP)',
        'RCnt': 'Reject Count -> (Reject + Presented (Pres))',
        'Safe': 'Safe',
        'Min': 'Min',
        'Max': 'Max',
        'Disp': 'Disp',
        'Pres': 'Presented notes to customer',
        'Retr': 'Retract',
        'A': 'AppL',
        'DevL': 'DevL',
        'St': 'Status - Indicates status of Logical cassette',
        'HWsens': 'HWsens',
        'Record_Type': 'Record Type'
    }