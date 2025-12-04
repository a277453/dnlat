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
    TransactionVisualizationRequest,
    ParseFilesRequest,
    PathRequest,
    FeedbackSubmission,
    TransactionAnalysisRequest
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
    DEBUG ENDPOINT: Returns raw list of all ZIP member names and basic info.
    Use this to understand ZIP structure and why matching may fail.
    No extraction or processing — just lists what's inside the archive.
    """
    logger.info("📥 Received request: /debug-zip-members")  
    try:
        zip_bytes = await file.read()
        logger.debug(f"Read {len(zip_bytes)} bytes from uploaded file: {file.filename}") 
        members = []
        
        # Try to open as standard ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
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


@router.post("/process-zip", response_model=FileCategorizationResponse)
async def process_zip_file(file: UploadFile = File(..., description="ZIP file to process"),mode: Optional[str] = Query(None, description="Processing mode (e.g., 'registry' to optimize for registry files)")):
    """
    Step 1: Receive and validate ZIP file upload
    """
    logger.info(" Received request to process ZIP file")
    if not file.filename.endswith('.zip'):
        logger.error(" Invalid file type — only ZIP allowed")
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
        logger.info("🔍 STEP 3: Checking for nested ZIP files.")
        t_nested_zip_start = time.perf_counter()

        nested_zip_files = [p for p in Path(extract_path).rglob('*.zip')]
        logger.info(f"Found nested ZIPs: {len(nested_zip_files)}")


        for nested_zip_path in nested_zip_files:
                logger.info(f"📁 Handling nested ZIP: {nested_zip_path.relative_to(extract_path)}")
            
                try:
                    with open(nested_zip_path, 'rb') as f:
                        nested_content = f.read()
                    # Extract nested zip into a directory with the same name
                    
                    nested_extract_path = extraction_service.extract_zip(nested_content)
                    all_files_after_nested = [p for p in Path(extract_path).rglob('*') if p.is_file()]
                    logger.info(f"  ✓ Nested ZIP extracted to {nested_extract_path}")
                    logger.info(f"Grand total files after nested ZIP extraction: {len(all_files_after_nested)}")
                except Exception as e:
                    logger.error(f"  Failed to extract nested ZIP {nested_zip_path.name}: {e}", exc_info=True)
                    
                
        t_nested_zip_end = time.perf_counter()
        logger.info(f"NESTED ZIP EXTRACTION TIME: {t_nested_zip_end - t_nested_zip_start:.4f} s")

        # ------------------ CATEGORIZATION + ACU EXTRACTION (COMBINED) ------------------
        # Extract ACU files first, then include in categorization result
        logger.info(f"🔧 Extracting ACU XML files (jdd*, x3*) directly from ZIP...")
        acu_logs = []
        t_cat_start = time.perf_counter()
        
        #  Extract ACU files from memory first to get their names
        try:
            acu_files = extract_from_zip_bytes(zip_content, acu_logs, target_prefixes=('jdd', 'x3'))
            xml_count = sum(1 for k in acu_files if not k.startswith('__xsd__'))
            xsd_count = sum(1 for k in acu_files if k.startswith('__xsd__'))
            logger.info(f"✓ ACU extraction: {xml_count} XML, {xsd_count} XSD files")

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
            logger.info(f"✓ Added {len(acu_files)} ACU files to final categories.")
        
        # Step 4: Run on-disk categorization, which will populate the SAME dictionary
        logger.info("Running on-disk categorization ...")
        categorization_service = CategorizationService()
        categorization_service.categorize_files(extract_path, file_categories, acu_filenames_to_exclude, mode=mode)
        
        t_cat_end = time.perf_counter()
        logger.debug(f"CATEGORIZATION + ACU EXTRACTION TIME: {t_cat_end - t_cat_start:.4f} s")

        # ------------------ SESSION CREATION TIMER ------------------
        logger.info("Creating/updating session")
        t_sess_start = time.perf_counter()
        set_processed_files_dir(str(extract_path))
        session_service.create_session(CURRENT_SESSION_ID, file_categories, extract_path)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extracted_files', acu_files)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extraction_logs', acu_logs)
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
    Extract ACU files from an uploaded ZIP for comparison purposes.
    """
    logger.info(f"📥 Received request: /extract-files/ for file: {file.filename}")  
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

# ============================================
# ACU PARSER ENDPOINTS
# ============================================

@router.get("/get-acu-files")
async def get_acu_files(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Get the list of ACU XML and XSD files extracted from the processed ZIP.
    
    Returns:
        dict: Contains 'acu_files' (list of XML files) and 'logs' (extraction logs)
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
        
        return {
            "acu_files": xml_files,
            "logs": acu_logs or [],
            "count": len(xml_files),
            "message": f"Found {len(xml_files)} ACU XML file(s)"
        }
        
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
    Parse ACU XML files using the advanced ACU parser.
    
    Args:
        files_to_parse: List of dicts with 'filename' (XML filename) keys
        session_id: Session ID containing extracted files
        
    Returns:
        dict: Parsed data as list of records and parsing logs
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
                    logs.append(f"✓ Parsed {filename}: {len(df)} records")
                    logger.info(f"Parsed {filename}: {len(df)} records")
                else:
                    logs.append(f"⚠️ No data extracted from {filename}")
                    logger.warning(f"No data extracted from {filename}")
                    
            except Exception as e:
                logs.append(f"✗ Failed to parse {filename}: {str(e)}")
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
    Get available file types from the processed ZIP
    """
    logger.info(f"📥 Received request: /available-file-types for session_id: {session_id}") 

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
    Select one or multiple file types and get available operations
    """
    logger.info(f"📥 Received request: /select-file-type for session_id: {session_id}")  

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
    Analyze customer journal files and extract transaction data
    """
    try:
        logger.info(f"🔍 Starting customer journal analysis for session: {session_id}")
        
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
        
        logger.info(f"📂 Found {len(journal_files)} customer journal file(s)")
        
        # Initialize analyzer
        analyzer = TransactionAnalyzerService()
        
        all_transactions_df = []
        source_files = []
        source_file_map = {}
        
        for journal_file in journal_files:
            logger.info(f"📖 Processing: {journal_file}")
            
            source_filename = Path(journal_file).stem
            source_files.append(source_filename)
            
            try:
                df = analyzer.parse_customer_journal(journal_file)
                
                if df is None or df.empty:
                    logger.debug(f"No transactions found in {source_filename}")
                    continue
                
                logger.info(f"✓ Found {len(df)} transactions")
                
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
        
        if 'Source_File' in combined_df.columns:
            combined_df = combined_df.rename(columns={'Source_File': 'Source File'})
        
        logger.info(f"✅ Total transactions extracted: {len(combined_df)}")
        logger.info(f"📁 Total source files: {len(source_files)}")
        
        if 'Source File' in combined_df.columns:
            unique_sources_in_data = combined_df['Source File'].unique().tolist()
            logger.debug(f"Source files in data: {unique_sources_in_data}")
            logger.debug(f"Source files list: {source_files}")
        
        transaction_records = combined_df.to_dict('records')
        
        session_service.update_session(session_id, 'transaction_data', transaction_records)
        session_service.update_session(session_id, 'source_files', source_files)
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
    Get all transactions with source file information
    """
    try:
        logger.info(f"Fetching transactions with source mapping for session: {session_id}")
        logger.debug("Entered /get-transactions-with-sources route")

        print(f"🔍 Getting transactions with sources for session: {session_id}")
        
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

        logger.info(f"Retrieved {len(transaction_data)} transactions from {len(source_files)} source files")
        logger.debug(f"Source file list: {source_files}")
        logger.debug(f"Source file map keys: {list(source_file_map.keys())}")

        print(f"✓ Found {len(transaction_data)} transactions from {len(source_files)} source files")
        
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
    Filter transactions by selected source files
    
    Request body example:
    {
        "source_files": ["CustomerJournal_1.txt", "CustomerJournal_2.txt"]
    }
    """
    try:
        logger.info(f"Request received: Filtering transactions for session {session_id}")
        logger.debug(f"Source files received for filtering: {source_files}")

        print(f"🔍 Filtering transactions by {len(source_files)} source file(s)")
        
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
        
        # Filter transactions by source file
        logger.debug("Applying source file filters to transaction list.")
        filtered_transactions = [
            txn for txn in transaction_data
            if txn.get('Source File') in source_files
        ]
        
        logger.info(f"Filtered transactions count: {len(filtered_transactions)}")
        print(f"✓ Filtered to {len(filtered_transactions)} transactions")
        
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
    Get transaction statistics from analyzed customer journals
    """
    try:
        logger.info(f"Request received: Get transaction statistics for session {session_id}")
        print(f"📊 Getting transaction statistics for session: {session_id}")
        
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
            
            stats.append({
                'Transaction Type': txn_type,
                'Total': total,
                'Successful': successful,
                'Unsuccessful': unsuccessful,
                'Success Rate': f"{(successful/total*100):.1f}%" if total > 0 else "0%"
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
    Compare UI flows of two transactions
    """
    try:
        logger.info(f"🔄 Comparing transactions: {txn1_id} vs {txn2_id}")

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

        logger.info(f"✓ Found both transactions: {txn1_id}, {txn2_id}")

        # Get file categories from session
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])

        logger.info(f"📂 Found {len(ui_journals)} UI journal file(s)")

        # Extract UI flows for both transactions
        ui_flow_1 = ["No screens in time range"]
        ui_flow_2 = ["No screens in time range"]

        if ui_journals:
            try:
                txn1_source_file = str(txn1_data.get('Source File', ''))
                txn2_source_file = str(txn2_data.get('Source File', ''))

                logger.debug(f"Transaction 1 source: {txn1_source_file}")
                logger.debug(f"Transaction 2 source: {txn2_source_file}")

                def extract_flow_for_transaction(txn_data, txn_source_file, txn_label):
                    flow_screens = ["No screens in time range"]

                    # Try to find matching UI journal first
                    matching_ui_journal = None
                    for ui_journal in ui_journals:
                        ui_journal_name = Path(ui_journal).stem
                        if ui_journal_name == txn_source_file:
                            matching_ui_journal = ui_journal
                            logger.info(f"✓ Found matching UI journal for {txn_label}: {ui_journal_name}")
                            break

                    # If no exact match, try all UI journals
                    ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals

                    for ui_journal_path in ui_journals_to_check:
                        logger.info(f"📖 Parsing UI journal for {txn_label}: {ui_journal_path}")

                        ui_df = parse_ui_journal(ui_journal_path)

                        if not ui_df.empty:
                            logger.info(f"✓ Parsed {len(ui_df)} UI events for {txn_label}")

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

                            # Extract flow
                            start_time = parse_time(txn_data['Start Time'])
                            end_time = parse_time(txn_data['End Time'])

                            if start_time and end_time:
                                logger.info(f"⏰ {txn_label} time range: {start_time} to {end_time}")
                                extracted_screens = processor.get_screen_flow(start_time, end_time)

                                if extracted_screens and len(extracted_screens) > 0:
                                    flow_screens = extracted_screens
                                    logger.info(f"✓ Flow extracted for {txn_label}: {len(flow_screens)} screens from {Path(ui_journal_path).stem}")
                                    break
                                else:
                                    logger.warning(f"⚠️ No screens found in time range for {txn_label} in {Path(ui_journal_path).stem}")
                            else:
                                logger.warning(f"⚠️ Invalid time range for {txn_label}")
                        else:
                            logger.warning(f"⚠️ Empty UI journal for {txn_label}: {Path(ui_journal_path).stem}")

                    return flow_screens

                ui_flow_1 = extract_flow_for_transaction(txn1_data, txn1_source_file, "Transaction 1")
                ui_flow_2 = extract_flow_for_transaction(txn2_data, txn2_source_file, "Transaction 2")

            except Exception as e:
                logger.error(f"❌ Error extracting UI flows: {e}", exc_info=True)
        else:
            logger.warning("⚠️ No UI journal files available")

        logger.info(f"📊 Transaction 1 flow: {len(ui_flow_1)} screens")
        logger.info(f"📊 Transaction 2 flow: {len(ui_flow_2)} screens")

        # --- Part C: LCS Matching and Analysis ---
        def find_lcs_matches(flow1, flow2):
            m, n = len(flow1), len(flow2)
            lcs_table = [[0] * (n + 1) for _ in range(m + 1)]
            for i in range(1, m + 1):
                for j in range(1, n + 1):
                    if flow1[i-1] == flow2[j-1]:
                        lcs_table[i][j] = lcs_table[i-1][j-1] + 1
                    else:
                        lcs_table[i][j] = max(lcs_table[i-1][j], lcs_table[i][j-1])
            matches1 = [False] * m
            matches2 = [False] * n
            i, j = m, n
            while i > 0 and j > 0:
                if flow1[i-1] == flow2[j-1]:
                    matches1[i-1] = True
                    matches2[j-1] = True
                    i -= 1
                    j -= 1
                elif lcs_table[i-1][j] > lcs_table[i][j-1]:
                    i -= 1
                else:
                    j -= 1
            logger.info(f"📌 LCS match computed: {sum(matches1)} matching screens")
            return matches1, matches2

        txn1_matches, txn2_matches = find_lcs_matches(ui_flow_1, ui_flow_2)

        # Generate detailed analysis
        detailed_analysis = ""
        try:
            def get_duration(txn_data):
                try:
                    start, end = txn_data['Start Time'], txn_data['End Time']
                    if isinstance(start, str):
                        start = datetime.strptime(start, '%H:%M:%S').time()
                    if isinstance(end, str):
                        end = datetime.strptime(end, '%H:%M:%S').time()
                    start_dt = datetime.combine(datetime.today(), start)
                    end_dt = datetime.combine(datetime.today(), end)
                    return (end_dt - start_dt).total_seconds()
                except Exception as e:
                    logger.warning(f"⚠️ Error computing duration: {e}")
                    return None

            txn1_duration, txn2_duration = get_duration(txn1_data), get_duration(txn2_data)
            analysis_lines = ["**Duration Analysis:**"]
            if txn1_duration is not None:
                analysis_lines.append(f"- Transaction 1: {txn1_duration:.1f} seconds")
                logger.debug(f"Transaction 1 duration: {txn1_duration:.1f} s")
            else:
                analysis_lines.append(f"- Transaction 1: Duration unavailable")
                logger.warning("Transaction 1 duration unavailable")
            if txn2_duration is not None:
                analysis_lines.append(f"- Transaction 2: {txn2_duration:.1f} seconds")
                logger.debug(f"Transaction 2 duration: {txn2_duration:.1f} s")
            else:
                analysis_lines.append(f"- Transaction 2: Duration unavailable")
                logger.warning("Transaction 2 duration unavailable")
            if txn1_duration and txn2_duration:
                diff = txn2_duration - txn1_duration
                if diff > 0:
                    analysis_lines.append(f"- Transaction 2 took {diff:.1f} seconds longer")
                elif diff < 0:
                    analysis_lines.append(f"- Transaction 1 took {abs(diff):.1f} seconds longer")
                else:
                    analysis_lines.append(f"- Both transactions took the same time")

            analysis_lines += ["", "**Screen Flow Analysis:**"]
            analysis_lines.append(f"- Transaction 1 screens: {len(ui_flow_1)}")
            analysis_lines.append(f"- Transaction 2 screens: {len(ui_flow_2)}")

            if ui_flow_1[0] != "No screens in time range" and ui_flow_2[0] != "No screens in time range":
                common_screens = set(ui_flow_1) & set(ui_flow_2)
                unique1 = set(ui_flow_1) - set(ui_flow_2)
                unique2 = set(ui_flow_2) - set(ui_flow_1)
                analysis_lines.append(f"- Common screens: {len(common_screens)}")
                analysis_lines.append(f"- Unique to Transaction 1: {len(unique1)}")
                analysis_lines.append(f"- Unique to Transaction 2: {len(unique2)}")

            analysis_lines += ["", "**Source Files:**"]
            analysis_lines.append(f"- Transaction 1: {txn1_data.get('Source File', 'Unknown')}")
            analysis_lines.append(f"- Transaction 2: {txn2_data.get('Source File', 'Unknown')}")
            if txn1_data.get('Source File') == txn2_data.get('Source File'):
                analysis_lines.append("- Both from the same source file")
            else:
                analysis_lines.append("- From different source files")

            detailed_analysis = "\n".join(analysis_lines)
            logger.info("📊 Detailed analysis generated successfully")

        except Exception as e:
            logger.error(f"⚠️ Error generating detailed analysis: {e}", exc_info=True)
            detailed_analysis = "Detailed analysis unavailable"

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
            "detailed_analysis": detailed_analysis
        }

        logger.info("✅ Comparison complete - returning response")
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comparison failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)}")


@router.get("/current-selection")
async def get_current_selection(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Get the currently selected file type(s)
    """
    try:
        logger.info(f"🔍 Getting current selection for session: {session_id}")

        if not session_service.session_exists(session_id):
            logger.error(f"❌ No session found for session_id: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No session found"
            )
        
        session = session_service.get_session(session_id)
        selected_types = session.get('selected_types', [])
        
        if not selected_types:
            logger.info(f"ℹ️ No file types selected yet for session: {session_id}")
            return {"selected_types": [], "message": "No file types selected yet"}
        
        logger.info(f"✓ Current selection for session {session_id}: {selected_types}")
        return {"selected_types": selected_types}
    
    except Exception as e:
        logger.exception(f"❌ Error retrieving current selection for session {session_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving current selection: {str(e)}"
        )


@router.get("/debug-session")
async def debug_session(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Debug endpoint to check session contents
    """
    try:
        logger.info(f"🔍 Debugging session: {session_id}")

        if not session_service.session_exists(session_id):
            logger.warning(f"❌ Session not found: {session_id}")
            return {
                "exists": False,
                "message": "Session not found"
            }
        
        session_data = session_service.get_session(session_id)
        logger.info(f"✓ Session found: {session_id}, keys: {list(session_data.keys())}")

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
        logger.exception(f"❌ Error debugging session {session_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging session: {str(e)}"
        )


@router.post("/visualize-individual-transaction-flow")
async def visualize_individual_transaction_flow(request: TransactionVisualizationRequest,session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Generate UI flow visualization for a single transaction
     Args:
        request: Request body containing the transaction ID
        session_id: Current session ID
        
    Returns:
        Dictionary containing:
        - transaction_data: Transaction details
        - ui_flow: List of screen names
        - has_flow: Boolean indicating if flow data exists
    """
    try:
        transaction_id = request.transaction_id
        logger.info(f"🔍 Visualizing flow for transaction: {transaction_id}")

        # Check if session exists
        if not session_service.session_exists(session_id):
            logger.warning(f"❌ No processed ZIP found for session: {session_id}")
            raise HTTPException(
                status_code=404,
                detail="No processed ZIP found. Please upload a ZIP file first."
            )

        # Get session data
        session_data = session_service.get_session(session_id)
        transaction_data = session_data.get('transaction_data')

        if not transaction_data:
            logger.warning(f"❌ No transaction data available for session: {session_id}")
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )

        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)

        # Check if transaction exists
        txn_exists = len(df[df['Transaction ID'] == transaction_id]) > 0
        if not txn_exists:
            logger.warning(f"❌ Transaction {transaction_id} not found in session: {session_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found."
            )

        # Get transaction details
        txn_data = df[df['Transaction ID'] == transaction_id].iloc[0]
        logger.info(f"✓ Found transaction: {transaction_id}")

        # Extract UI flow
        ui_flow_screens = ["No flow data"]
        has_flow = False

        # Get file categories
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        logger.info(f"📂 Found {len(ui_journals)} UI journal file(s)")

        if ui_journals:
            try:
                txn_source_file = str(txn_data.get('Source File', ''))
                logger.info(f"📂 Transaction source file: {txn_source_file}")

                matching_ui_journal = None
                for ui_journal in ui_journals:
                    ui_journal_name = Path(ui_journal).stem
                    if ui_journal_name == txn_source_file:
                        matching_ui_journal = ui_journal
                        logger.info(f"✓ Found matching UI journal: {ui_journal_name}")
                        break

                ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals

                for ui_journal_path in ui_journals_to_check:
                    logger.debug(f"📖 Parsing UI journal: {ui_journal_path}")
                    ui_df = parse_ui_journal(ui_journal_path)

                    if not ui_df.empty:
                        logger.info(f"✓ Parsed {len(ui_df)} UI events")
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
                            logger.info(f"⏰ Time range: {start_time} to {end_time}")
                            ui_flow_screens = processor.get_screen_flow(start_time, end_time)

                            if ui_flow_screens and len(ui_flow_screens) > 0:
                                has_flow = True
                                logger.info(f"✓ Flow extracted: {len(ui_flow_screens)} screens from {Path(ui_journal_path).stem}")
                                break
                            else:
                                logger.warning(f"⚠️ No screens found in time range for {Path(ui_journal_path).stem}")
                        else:
                            logger.warning("⚠️ Invalid time range")
                    else:
                        logger.warning(f"⚠️ Empty UI journal: {Path(ui_journal_path).stem}")

            except Exception as e:
                logger.exception(f"❌ Error extracting UI flow: {str(e)}")
        else:
            logger.warning("⚠️ No UI journal files available")

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
            "num_events": len(ui_flow_screens) if ui_flow_screens else 0
        }

        logger.info("✅ Visualization data prepared")
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Visualization failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Visualization failed: {str(e)}"
        )

@router.post("/generate-consolidated-flow")
async def generate_consolidated_flow(source_file: str = Body(...),transaction_type: str = Body(...),session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Generate consolidated flow visualization for all transactions of a specific type
    from a specific source file
    """
    try:
        logger.info(f"🔄 Starting consolidated flow generation for type '{transaction_type}' from source '{source_file}'")
        logger.debug(f"Session ID received: {session_id}")
        
        print(f"🔄 Generating consolidated flow for {transaction_type} from {source_file}")
        
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
        logger.debug(f"Applying filters → Source File: {source_file}, Transaction Type: {transaction_type}")
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
        
        logger.info(f"✓ {len(filtered_df)} transactions matched filters")
        print(f"✓ Found {len(filtered_df)} transactions")
        
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
        print(f"✓ Found matching UI journal: {matching_ui_journal}")
        
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
        print(f"✓ Parsed UI journal with {len(ui_df)} events")
        
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
            
            logger.debug(f"Times parsed → Start: {start_time}, End: {end_time}")
            
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
            logger.error("UI flow extraction failed — no flows found")
            raise HTTPException(
                status_code=404,
                detail="No UI flow data could be extracted for these transactions"
            )
        
        logger.info(f"✓ Extracted flows for {len(transaction_flows)} transactions")
        logger.debug(f"Unique screens: {len(all_screens)}, Unique transitions: {len(transitions)}")
        
        print(f"✓ Extracted flows for {len(transaction_flows)} transactions")
        print(f"✓ Found {len(all_screens)} unique screens")
        print(f"✓ Found {len(transitions)} unique transitions")
        
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


# Then replace the endpoint:
@router.post("/analyze-transaction-llm")
async def analyze_transaction_llm(request: TransactionAnalysisRequest,session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Analyze a transaction log using LLM (Ollama) for anomaly detection
    """
    try:
        transaction_id = request.transaction_id
        logger.info(f"🤖 Analyzing transaction with LLM: {transaction_id}")
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
        
        logger.info(f"✓ Found transaction log ({len(transaction_log)} characters)")

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
            
            logger.info("🤖 Calling Ollama model...")
            logger.debug(f"LLM messages payload: {messages}")
            
            response = ollama.chat(model="llama3_log_analyzer", messages=messages)
            raw_response = response['message']['content'].strip()
            logger.info(f"✓ LLM analysis complete ({len(raw_response)} characters)")
            
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


@router.post("/submit-llm-feedback")
async def submit_llm_feedback(feedback: FeedbackSubmission,session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Submit feedback for LLM analysis
    """
    try:
        logger.info(f"📝 Submitting feedback for transaction: {feedback.transaction_id}")
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
            logger.info(f"✓ Feedback saved to file: {feedback_file}")
        except Exception as e:
            logger.error(f"⚠️ Could not save feedback to file {feedback_file}: {str(e)}")
        
        # Also store in session for immediate retrieval
        if not session_service.session_exists(session_id):
            session_service.create_session(session_id)
            logger.debug(f"Created new session: {session_id}")
        
        session_data = session_service.get_session(session_id)
        
        if 'feedback_data' not in session_data:
            session_data['feedback_data'] = []
        
        session_data['feedback_data'].append(feedback_record)
        session_service.update_session(session_id, session_data)
        logger.info(f"✓ Feedback stored in session for session_id: {session_id}")
        
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
    Get all feedback for a specific transaction
    """
    try:
        logger.info(f"📖 Retrieving feedback for transaction: {transaction_id}")
        
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
                logger.error(f"⚠️ Could not read feedback file {feedback_file}: {str(e)}")
        
        logger.info(f"✓ Found {len(all_feedback)} feedback record(s) for transaction {transaction_id}")
        
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


