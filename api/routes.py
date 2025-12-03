from fastapi import APIRouter, UploadFile, File, HTTPException, Query
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
    FeedbackSubmission, # This seems to be a duplicate import
    TransactionAnalysisRequest
)
from modules.extraction import ZipExtractionService, extract_from_directory, extract_from_zip_bytes
from modules.xml_parser_logic import parse_xml_to_dataframe
from pathlib import Path
from typing import Dict, List, Optional
import shutil
from fastapi import Body
import os
import pandas as pd
from modules.ui_journal_processor import UIJournalProcessor, parse_ui_journal
from datetime import datetime
from collections import defaultdict
from fastapi.logger import logger
import zipfile
import io
import logging
import json
import time

logging.basicConfig (
    level=logging.INFO,  # Show INFO level logs
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


router = APIRouter()

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
    try:
        zip_bytes = await file.read()
        members = []
        
        # Try to open as standard ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                for info in zf.infolist():
                    members.append({
                        "path": info.filename,
                        "basename": os.path.basename(info.filename),
                        "is_dir": info.is_dir(),
                        "compressed_size": info.compress_size,
                        "uncompressed_size": info.file_size,
                        "compress_type": info.compress_type
                    })
        except zipfile.BadZipFile as e:
            return {
                "status": "error",
                "error": f"BadZipFile: {str(e)}",
                "note": "Archive may be corrupted or use non-standard format. Low-level extractor will be needed.",
                "members": []
            }
        
        # Filter for potential ACU files
        xml_files = [m for m in members if m["basename"].lower().endswith('.xml')]
        xsd_files = [m for m in members if m["basename"].lower().endswith('.xsd')]
        
        # Check which would match with current patterns (jdd, x3)
        matching_xml = [m for m in xml_files if m["basename"].lower().startswith(('jdd', 'x3'))]
        matching_xsd = [m for m in xsd_files if m["basename"].lower().startswith(('jdd', 'x3'))]
        
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
    print(f"✓ Processed files directory set to: {directory}")

from fastapi.logger import logger

@router.post("/process-zip", response_model=FileCategorizationResponse)
async def process_zip_file(
    file: UploadFile = File(..., description="ZIP file to process"),
    mode: Optional[str] = Query(None, description="Processing mode (e.g., 'registry' to optimize for registry files)")
):
    """
    Step 1: Receive and validate ZIP file upload
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(
            status_code=400,
            detail="Only ZIP files are accepted"
        )
    
    try:
        

        # -----------------------------------------------------------
        # ⏱️ START TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        start_time = time.perf_counter()
        # -----------------------------------------------------------

        # ------------------ FILE READ TIMER ------------------
        t_file_start = time.perf_counter()
        zip_content = await file.read()  # read only once
        t_file_end = time.perf_counter()
        logger.info(f"FILE READ TIME: {t_file_end - t_file_start:.4f} s")

        # ------------------ ZIP EXTRACTION TIMER ------------------
        t_zip_start = time.perf_counter()
        extraction_service = ZipExtractionService()
        extract_path = extraction_service.extract_zip(zip_content, is_nested=False)
        t_zip_end = time.perf_counter()
        logger.info(f"ZIP EXTRACTION TIME: {t_zip_end - t_zip_start:.4f} s")

        # ------------------ CATEGORIZATION + ACU EXTRACTION (COMBINED) ------------------ #
        # Extract ACU files first, then include in categorization result
        logger.info(f"🔧 Extracting ACU XML files (jdd*, x3*) directly from ZIP...")
        acu_logs = []
        t_cat_start = time.perf_counter()
        
        # Step 1: Extract ACU files from memory first to get their names
        try:
            acu_files = extract_from_zip_bytes(zip_content, acu_logs, target_prefixes=('jdd', 'x3'))
            xml_count = sum(1 for k in acu_files if not k.startswith('__xsd__'))
            xsd_count = sum(1 for k in acu_files if k.startswith('__xsd__'))
            logger.info(f"✓ ACU extraction: {xml_count} XML, {xsd_count} XSD files")
            # Get a set of base filenames for exclusion during disk scan
            acu_filenames_to_exclude = {os.path.basename(p) for p in acu_files.keys()}
        except Exception as e:
            logger.error(f"❌ Error extracting ACU files: {str(e)}")
            
            acu_files = {}
            acu_filenames_to_exclude = set()
            acu_logs.append(f"Error: {str(e)}")
        
        # Step 2: Categorize files from the extracted directory, excluding ACU files found in memory
        # Initialize the categories dictionary here
        file_categories = {
            'customer_journals': [], 'ui_journals': [], 'trc_trace': [],
            'trc_error': [], 'registry_files': [], 'acu_files': [], 'unidentified': []
        }
        
        # Step 3: Add the in-memory extracted ACU files to the categories FIRST
        if acu_files:
            file_categories['acu_files'] = list(acu_files.keys())
            logger.info(f"✓ Added {len(acu_files)} ACU files to final categories.")
        
        # Step 4: Run on-disk categorization for all other files, populating the SAME dictionary
        categorization_service = CategorizationService()
        categorization_service.categorize_files(extract_path, file_categories, exclude_files=acu_filenames_to_exclude, mode=mode)
        
        t_cat_end = time.perf_counter()
        logger.info(f"CATEGORIZATION + ACU EXTRACTION TIME: {t_cat_end - t_cat_start:.4f} s")

        # ------------------ SESSION CREATION TIMER ------------------
        t_sess_start = time.perf_counter()
        set_processed_files_dir(str(extract_path))
        session_service.create_session(CURRENT_SESSION_ID, file_categories, extract_path)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extracted_files', acu_files)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extraction_logs', acu_logs)
        t_sess_end = time.perf_counter()
        logger.info(f"SESSION SAVE TIME: {t_sess_end - t_sess_start:.4f} s")

        # ------------------ PROCESSING TIMER ------------------
        t_proc_start = time.perf_counter()
        processing_service = ProcessingService()
        result = processing_service.prepare_response(file_categories, extract_path)
        result.acu_extraction_logs = acu_logs
        t_proc_end = time.perf_counter()
        logger.info(f"PROCESSING TIME: {t_proc_end - t_proc_start:.4f} s")

        # -----------------------------------------------------------
        # ⏱️ END TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        end_time = time.perf_counter()
        logger.info(f"TOTAL ZIP PROCESSING TIME: {end_time - start_time:.4f} s")
        # -----------------------------------------------------------

        return result
        
    except Exception as e:
        import traceback
        logger.error(f"❌ ERROR in process_zip: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error processing ZIP file: {str(e)}"
        )


'''@router.post("/process-zip", response_model=FileCategorizationResponse)
async def process_zip_file(
    file: UploadFile = File(..., description="ZIP file to process")
):
    """
    Step 1: Receive and validate ZIP file upload
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(
            status_code=400,
            detail="Only ZIP files are accepted"
        )
    
    try:
        # -----------------------------------------------------------
        # ⏱️ START TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        import time
        start_time = time.time()
        # -----------------------------------------------------------

        # Read the uploaded file
        zip_content = await file.read()
        
        # Step 2: Extract
        extraction_service = ZipExtractionService()
        extract_path = extraction_service.extract_zip(zip_content)


        #-----added Measure how long it takes to scan all files----------
        #how long backend took to find ALL files inside the extracted ZIP directory.
        #scan_start = time.time()

        #file_count = 0
        #or root, dirs, files in os.walk(extract_path):
          #  file_count += len(files)

        #scan_end = time.time()
        #print(f"📂 FILE SCAN TIME: {scan_end - scan_start:.4f} seconds ({file_count} files found)")
        # ---------------------------------------------

        
        # Step 3: Categorize
        categorization_service = CategorizationService()
        file_categories = categorization_service.categorize_files(extract_path)

        #Measure categorization time
        cat_start = time.time()

        file_categories = categorization_service.categorize_files(extract_path)

        cat_end = time.time()
        print(f"🗂 FILE CATEGORIZATION TIME: {cat_end - cat_start:.4f} seconds")
        
        # EXTRACT ACU FILES AUTOMATICALLY USING ADVANCED ACU ZIP EXTRACTOR
        print(f"🔧 Searching for ACU XML files (jdd*, x3*) directly from ZIP...")
        
        acu_logs = []
        try:
            # Use the zip extractor to get ACU files directly from the uploaded ZIP bytes
            acu_files = extract_from_zip_bytes(zip_content, acu_logs, target_prefixes=('jdd', 'x3'))
            
            xml_count = sum(1 for k in acu_files if not k.startswith('__xsd__'))
            xsd_count = sum(1 for k in acu_files if k.startswith('__xsd__'))
            
            print(f"  ✓ ACU Extraction complete: {xml_count} XML files, {xsd_count} XSD files")
            
            if acu_files:
                acu_files_dict = acu_files
                print(f"  ✓ Found {xml_count} ACU XML files with documentation")
            else:
                print(f"  ⚠️ No ACU files found in the ZIP for prefixes ('jdd', 'x3')")
                acu_files_dict = {}
        except Exception as e:
            print(f"  ❌ Error extracting ACU files: {str(e)}")
            acu_files_dict = {}
            acu_logs.append(f"Error: {str(e)}")
        
        # Set processed files directory for registry endpoints
        set_processed_files_dir(str(extract_path))
        
        # Debug output
        print(f"🔍 DEBUG: About to create session")
        print(f"📁 File categories: {list(file_categories.keys())}")
        print(f"📊 File counts: {dict((k, len(v)) for k, v in file_categories.items())}")
        
        # Step 4: Store in session
        session_service.create_session(CURRENT_SESSION_ID, file_categories, extract_path)
        
        # Store ACU files dict and logs
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extracted_files', acu_files_dict)
        session_service.update_session(CURRENT_SESSION_ID, 'acu_extraction_logs', acu_logs)

        print(f"✅ DEBUG: Session created successfully")
        print(f"🔍 DEBUG: Verifying session exists: {session_service.session_exists(CURRENT_SESSION_ID)}")
        
        # Step 5: Process and return results
        processing_service = ProcessingService()
        result = processing_service.prepare_response(file_categories, extract_path)
        result.acu_extraction_logs = acu_logs
        
        # -----------------------------------------------------------
        # ⏱️ END TOTAL TIME MEASUREMENT
        # -----------------------------------------------------------
        end_time = time.time()
        print(f" TOTAL ZIP PROCESSING TIME: {end_time - start_time:.4f} seconds")
        # -----------------------------------------------------------

        return result
        
    except Exception as e:
        import traceback
        print(f"❌ ERROR in process_zip: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error processing ZIP file: {str(e)}"
        )'''


@router.post("/extract-files/")
async def extract_files_from_zip(file: UploadFile = File(...)):
    """
    Extract ACU files from an uploaded ZIP for comparison purposes.
    """
    try:
        if not file.filename.endswith('.zip'):
            raise HTTPException(
                status_code=400,
                detail="Only ZIP files are accepted"
            )
        
        zip_content = await file.read()
        acu_logs = []
        acu_files = extract_from_zip_bytes(zip_content, acu_logs, target_prefixes=('jdd', 'x3'))
        
        if not acu_files:
            return {
                "files": {},
                "logs": acu_logs,
                "message": "No ACU files found in the uploaded ZIP"
            }
        
        return {
            "files": acu_files,
            "logs": acu_logs,
            "message": f"Successfully extracted {len(acu_files)} file(s)"
        }
        
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Error extracting files: {str(e)}\n{traceback.format_exc()}"
        )

import sys


@router.get("/get-acu-files")
async def get_acu_files(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Return ACU extracted files and logs stored in the current session (if any).
    """
    try:
        if not session_service.session_exists(session_id):
            raise HTTPException(status_code=404, detail="No processed ZIP found. Please upload a ZIP file first.")

        session = session_service.get_session(session_id)
        acu_files = session.get('acu_extracted_files', {}) if session else {}
        acu_logs = session.get('acu_extraction_logs', []) if session else []

        # Filter XML files only (exclude XSD)
        xml_files = {k: v for k, v in acu_files.items() if not k.startswith('__xsd__')} if isinstance(acu_files, dict) else []

        return {
            "files": acu_files,
            "xml_files": list(xml_files.keys()) if isinstance(xml_files, dict) else [],
            "logs": acu_logs,
            "xml_count": len(xml_files) if isinstance(xml_files, dict) else 0,
            "xsd_count": sum(1 for k in acu_files.keys() if k.startswith('__xsd__')) if isinstance(acu_files, dict) else 0
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unable to retrieve ACU files from session: {str(e)}")


@router.post("/parse-acu-files")
async def parse_acu_files_from_session(
    files_to_parse: List[dict] = Body(...),
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Parse ACU XML files using content stored in session.
    
    Args:
        files_to_parse: List of dicts with 'filename' keys
        session_id: Session ID containing extracted files
        
    Returns:
        dict: Parsed data as list of records and parsing logs
    """
    try:
        if not session_service.session_exists(session_id):
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload a ZIP file first."
            )
        
        # Get all extracted ACU files from session
        session = session_service.get_session(session_id)
        acu_files = session.get('acu_extracted_files', {})
        
        if not acu_files:
            raise HTTPException(
                status_code=400,
                detail="No ACU files found in the processed package."
            )
        
        all_parsed_data = []
        logs = []
        
        for file_info in files_to_parse:
            filename = file_info.get('filename')
            
            if not filename:
                logs.append("Skipped: No filename provided")
                continue
            
            # Look up the file content
            xml_content = acu_files.get(filename)
            
            if not xml_content:
                logs.append(f"File not found in extracted package: {filename}")
                continue
            
            # Look for matching XSD
            xsd_content = None
            xml_basename = os.path.splitext(os.path.basename(filename))[0].lower()
            
            # Try different XSD key patterns
            possible_xsd_keys = [
                f'__xsd__{xml_basename}',
                f'__xsd__jdd_{xml_basename}',
                f'__xsd__x3_{xml_basename}',
            ]
            
            for xsd_key in possible_xsd_keys:
                if xsd_key in acu_files:
                    xsd_content = acu_files[xsd_key]
                    break
            
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
                else:
                    logs.append(f"⚠️ No data extracted from {filename}")
                    
            except Exception as e:
                logs.append(f"✗ Failed to parse {filename}: {str(e)}")
        
        return {
            "data": all_parsed_data,
            "logs": logs,
            "total_records": len(all_parsed_data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error parsing ACU files: {str(e)}"
        )

@router.get("/available-file-types", response_model=AvailableFileTypesResponse)
async def get_available_file_types(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Get available file types from the processed ZIP
    """
    # Check if session exists
    if not session_service.session_exists(session_id):
        raise HTTPException(
            status_code=404,
            detail="No processed ZIP found. Please upload a ZIP file first."
        )
    
    # Get file categories
    file_categories = session_service.get_file_categories(session_id)
    
    if not file_categories:
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
    
    return AvailableFileTypesResponse(
        available_types=available_types,
        type_details=type_details
    )

@router.post("/select-file-type")
async def select_file_type(
    request: FileTypeSelectionRequest,
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Select one or multiple file types and get available operations
    """
    # Check if session exists
    if not session_service.session_exists(session_id):
        raise HTTPException(
            status_code=404,
            detail="No processed ZIP found. Please upload a ZIP file first."
        )
    
    # Get file categories
    file_categories = session_service.get_file_categories(session_id)
    
    if not file_categories:
        raise HTTPException(
            status_code=404,
            detail="No file categories found"
        )
    
    # Get selected file types - convert enum to string
    try:
        selected_types = [ft.value if hasattr(ft, 'value') else str(ft) for ft in request.file_types]
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file types format: {str(e)}"
        )
    
    # Validate all selected types
    for selected_type in selected_types:
        if selected_type not in file_categories or len(file_categories[selected_type]) == 0:
            raise HTTPException(
                status_code=400,
                detail=f"No files found for type: {selected_type}"
            )
    
    # Store selected types in session
    session_service.update_session(session_id, 'selected_types', selected_types)
    
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
        print(f"🔍 Starting customer journal analysis for session: {session_id}")
        
        # Check if session exists
        if not session_service.session_exists(session_id):
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload a ZIP file first."
            )
        
        # Get file categories from session
        file_categories = session_service.get_file_categories(session_id)
        journal_files = file_categories.get('customer_journals', [])
        
        if not journal_files:
            raise HTTPException(
                status_code=400,
                detail="No customer journal files found in the uploaded package."
            )
        
        print(f"📂 Found {len(journal_files)} customer journal file(s)")
        
        # Initialize analyzer
        analyzer = TransactionAnalyzerService()
        
        # Parse all journal files and collect transactions
        all_transactions_df = []
        source_files = []
        source_file_map = {}
        
        for journal_file in journal_files:
            print(f"📖 Processing: {journal_file}")
            
            # Get the source filename - use the same format as in the DataFrame
            source_filename = Path(journal_file).stem  # Match what parse_customer_journal uses
            source_files.append(source_filename)
            
            try:
                # parse_customer_journal returns a DataFrame
                df = analyzer.parse_customer_journal(journal_file)
                
                if df is None or df.empty:
                    print(f"  ⚠️ No transactions found in {source_filename}")
                    continue
                
                print(f"  ✓ Found {len(df)} transactions")
                
                # The DataFrame already has 'Source_File' column set by parse_customer_journal
                # which uses Path(file_path).stem - same as our source_filename above
                
                # Add the dataframe to our collection
                all_transactions_df.append(df)
                
                # Track which transactions came from this file
                if 'Transaction ID' in df.columns:
                    file_transactions_ids = df['Transaction ID'].tolist()
                    source_file_map[source_filename] = file_transactions_ids
                
            except Exception as e:
                print(f"  ❌ Error processing {journal_file}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        if not all_transactions_df:
            raise HTTPException(
                status_code=400,
                detail="No transactions could be extracted from the customer journal files."
            )
        
        # Combine all dataframes
        combined_df = pd.concat(all_transactions_df, ignore_index=True)
        
        # Rename 'Source_File' to 'Source File' (with space) for consistency
        if 'Source_File' in combined_df.columns:
            combined_df = combined_df.rename(columns={'Source_File': 'Source File'})
        
        print(f"✅ Total transactions extracted: {len(combined_df)}")
        print(f"📁 Total source files: {len(source_files)}")
        
        # Debug: Print sample of source files in the data
        if 'Source File' in combined_df.columns:
            unique_sources_in_data = combined_df['Source File'].unique().tolist()
            print(f"🔍 DEBUG - Source files in data: {unique_sources_in_data}")
            print(f"🔍 DEBUG - Source files list: {source_files}")
        
        # Convert DataFrame to list of dictionaries for storage
        transaction_records = combined_df.to_dict('records')
        
        # Store in session
        session_service.update_session(session_id, 'transaction_data', transaction_records)
        session_service.update_session(session_id, 'source_files', source_files)
        session_service.update_session(session_id, 'source_file_map', source_file_map)
        
        # Generate statistics
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
        print(f"🔍 Getting transactions with sources for session: {session_id}")
        
        if not session_service.session_exists(session_id):
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload and analyze files first."
            )
        
        session_data = session_service.get_session(session_id)
        
        transaction_data = session_data.get('transaction_data', [])
        source_files = session_data.get('source_files', [])
        source_file_map = session_data.get('source_file_map', {})
        
        print(f"✓ Found {len(transaction_data)} transactions from {len(source_files)} source files")
        
        return {
            'source_files': source_files,
            'source_file_map': source_file_map,
            'all_transactions': transaction_data,
            'total_transactions': len(transaction_data)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving transactions: {str(e)}"
        )

@router.post("/filter-transactions-by-sources")
async def filter_transactions_by_sources(
    source_files: List[str] = Body(..., embed=True),
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Filter transactions by selected source files
    
    Request body example:
    {
        "source_files": ["CustomerJournal_1.txt", "CustomerJournal_2.txt"]
    }
    """
    try:
        print(f"🔍 Filtering transactions by {len(source_files)} source file(s)")
        
        if not session_service.session_exists(session_id):
            raise HTTPException(
                status_code=404,
                detail="No session found."
            )
        
        session_data = session_service.get_session(session_id)
        transaction_data = session_data.get('transaction_data', [])
        
        if not transaction_data:
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )
        
        # Filter transactions by source file
        filtered_transactions = [
            txn for txn in transaction_data
            if txn.get('Source File') in source_files
        ]
        
        print(f"✓ Filtered to {len(filtered_transactions)} transactions")
        
        return {
            'transactions': filtered_transactions,
            'count': len(filtered_transactions),
            'source_files': source_files
        }
    
    except HTTPException:
        raise
    except Exception as e:
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
        print(f"📊 Getting transaction statistics for session: {session_id}")
        
        if not session_service.session_exists(session_id):
            raise HTTPException(
                status_code=404,
                detail="No session found. Please upload and analyze files first."
            )
        
        session_data = session_service.get_session(session_id)
        transaction_data = session_data.get('transaction_data')
        
        if not transaction_data:
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame(transaction_data)
        
        # Generate statistics by transaction type
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
        
        return {
            'statistics': stats,
            'total_transactions': len(transaction_data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error generating statistics: {str(e)}"
        )

@router.post("/compare-transactions-flow")
async def compare_transactions_flow(
    txn1_id: str = Body(...),
    txn2_id: str = Body(...),
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Compare UI flows of two transactions
    """
    try:
        print(f"🔄 Comparing transactions: {txn1_id} vs {txn2_id}")
        
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
                detail="No transaction data available. Please analyze customer journals first."
            )
        
        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)
        
        # Check if both transactions exist
        txn1_exists = len(df[df['Transaction ID'] == txn1_id]) > 0
        txn2_exists = len(df[df['Transaction ID'] == txn2_id]) > 0
        
        if not txn1_exists:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {txn1_id} not found"
            )
        
        if not txn2_exists:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {txn2_id} not found"
            )
        
        # Get transaction details
        txn1_data = df[df['Transaction ID'] == txn1_id].iloc[0]
        txn2_data = df[df['Transaction ID'] == txn2_id].iloc[0]
        
        print(f"✓ Found both transactions")
        print(f"  Transaction 1: {txn1_id} - {txn1_data['Transaction Type']} ({txn1_data['End State']})")
        print(f"  Transaction 2: {txn2_id} - {txn2_data['Transaction Type']} ({txn2_data['End State']})")
        
        # Get file categories from session
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        
        print(f"📂 Found {len(ui_journals)} UI journal file(s)")
        
        # Extract UI flows for both transactions
        ui_flow_1 = ["No screens in time range"]
        ui_flow_2 = ["No screens in time range"]
        
        if ui_journals:
            try:
                # Get source files for both transactions
                txn1_source_file = str(txn1_data.get('Source File', ''))
                txn2_source_file = str(txn2_data.get('Source File', ''))
                
                print(f"📂 Transaction 1 source: {txn1_source_file}")
                print(f"📂 Transaction 2 source: {txn2_source_file}")
                
                # Function to extract flow for a transaction
                def extract_flow_for_transaction(txn_data, txn_source_file, txn_label):
                    flow_screens = ["No screens in time range"]
                    
                    # Try to find matching UI journal first
                    matching_ui_journal = None
                    for ui_journal in ui_journals:
                        ui_journal_name = Path(ui_journal).stem
                        if ui_journal_name == txn_source_file:
                            matching_ui_journal = ui_journal
                            print(f"✓ Found matching UI journal for {txn_label}: {ui_journal_name}")
                            break
                    
                    # If no exact match, try all UI journals
                    ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals
                    
                    for ui_journal_path in ui_journals_to_check:
                        print(f"📖 Parsing UI journal for {txn_label}: {ui_journal_path}")
                        
                        ui_df = parse_ui_journal(ui_journal_path)
                        
                        if not ui_df.empty:
                            print(f"✓ Parsed {len(ui_df)} UI events for {txn_label}")
                            
                            # Create processor
                            processor = UIJournalProcessor(ui_journal_path)
                            processor.df = ui_df
                            
                            # Convert times
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
                                print(f"⏰ {txn_label} time range: {start_time} to {end_time}")
                                extracted_screens = processor.get_screen_flow(start_time, end_time)
                                
                                if extracted_screens and len(extracted_screens) > 0:
                                    flow_screens = extracted_screens
                                    print(f"✓ Flow extracted for {txn_label}: {len(flow_screens)} screens from {Path(ui_journal_path).stem}")
                                    break  # Found the flow, stop checking other files
                                else:
                                    print(f"⚠️ No screens found in time range for {txn_label} in {Path(ui_journal_path).stem}")
                            else:
                                print(f"⚠️ Invalid time range for {txn_label}")
                        else:
                            print(f"⚠️ Empty UI journal for {txn_label}: {Path(ui_journal_path).stem}")
                    
                    return flow_screens
                
                # Extract flows for both transactions
                ui_flow_1 = extract_flow_for_transaction(txn1_data, txn1_source_file, "Transaction 1")
                ui_flow_2 = extract_flow_for_transaction(txn2_data, txn2_source_file, "Transaction 2")
                
            except Exception as e:
                print(f"❌ Error extracting UI flows: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print("⚠️ No UI journal files available")
        
        print(f"📊 Transaction 1 flow: {len(ui_flow_1)} screens")
        print(f"📊 Transaction 2 flow: {len(ui_flow_2)} screens")
        
        # Find matches using LCS (Longest Common Subsequence)
        def find_lcs_matches(flow1, flow2):
            """Find screens that appear in the same relative order in both flows using LCS"""
            m, n = len(flow1), len(flow2)
            lcs_table = [[0] * (n + 1) for _ in range(m + 1)]
            
            # Fill LCS table
            for i in range(1, m + 1):
                for j in range(1, n + 1):
                    if flow1[i-1] == flow2[j-1]:
                        lcs_table[i][j] = lcs_table[i-1][j-1] + 1
                    else:
                        lcs_table[i][j] = max(lcs_table[i-1][j], lcs_table[i][j-1])
            
            # Backtrack to find which screens are part of LCS
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
            
            return matches1, matches2
        
        # Get matches
        txn1_matches, txn2_matches = find_lcs_matches(ui_flow_1, ui_flow_2)
        
        # Generate detailed analysis
        detailed_analysis = ""
        try:
            # Duration analysis
            def get_duration(txn_data):
                try:
                    if 'Start Time' in txn_data and 'End Time' in txn_data:
                        start = txn_data['Start Time']
                        end = txn_data['End Time']
                        
                        # Handle both time objects and strings
                        if isinstance(start, str):
                            start = datetime.strptime(start, '%H:%M:%S').time()
                        if isinstance(end, str):
                            end = datetime.strptime(end, '%H:%M:%S').time()
                        
                        start_dt = datetime.combine(datetime.today(), start)
                        end_dt = datetime.combine(datetime.today(), end)
                        
                        return (end_dt - start_dt).total_seconds()
                except:
                    return None
                return None
            
            txn1_duration = get_duration(txn1_data)
            txn2_duration = get_duration(txn2_data)
            
            analysis_lines = []
            analysis_lines.append("**Duration Analysis:**")
            
            if txn1_duration is not None:
                analysis_lines.append(f"- Transaction 1: {txn1_duration:.1f} seconds")
            else:
                analysis_lines.append(f"- Transaction 1: Duration unavailable")
            
            if txn2_duration is not None:
                analysis_lines.append(f"- Transaction 2: {txn2_duration:.1f} seconds")
            else:
                analysis_lines.append(f"- Transaction 2: Duration unavailable")
            
            if txn1_duration is not None and txn2_duration is not None:
                duration_diff = txn2_duration - txn1_duration
                if duration_diff > 0:
                    analysis_lines.append(f"- Transaction 2 took {duration_diff:.1f} seconds longer")
                elif duration_diff < 0:
                    analysis_lines.append(f"- Transaction 1 took {abs(duration_diff):.1f} seconds longer")
                else:
                    analysis_lines.append(f"- Both transactions took the same time")
            
            analysis_lines.append("")
            analysis_lines.append("**Screen Flow Analysis:**")
            analysis_lines.append(f"- Transaction 1 screens: {len(ui_flow_1)}")
            analysis_lines.append(f"- Transaction 2 screens: {len(ui_flow_2)}")
            
            # Calculate screen overlap
            if ui_flow_1[0] != "No screens in time range" and ui_flow_2[0] != "No screens in time range":
                common_screens = set(ui_flow_1) & set(ui_flow_2)
                unique_to_txn1 = set(ui_flow_1) - set(ui_flow_2)
                unique_to_txn2 = set(ui_flow_2) - set(ui_flow_1)
                
                analysis_lines.append(f"- Common screens: {len(common_screens)}")
                analysis_lines.append(f"- Unique to Transaction 1: {len(unique_to_txn1)}")
                analysis_lines.append(f"- Unique to Transaction 2: {len(unique_to_txn2)}")
            
            analysis_lines.append("")
            analysis_lines.append("**Source Files:**")
            analysis_lines.append(f"- Transaction 1: {txn1_data.get('Source File', 'Unknown')}")
            analysis_lines.append(f"- Transaction 2: {txn2_data.get('Source File', 'Unknown')}")
            
            if txn1_data.get('Source File') == txn2_data.get('Source File'):
                analysis_lines.append(f"- Both from the same source file")
            else:
                analysis_lines.append(f"- From different source files")
            
            detailed_analysis = "\n".join(analysis_lines)
            
        except Exception as e:
            print(f"⚠️ Error generating detailed analysis: {str(e)}")
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
        
        print(f"✅ Comparison complete - returning response")
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Comparison failed: {str(e)}"
        )

@router.get("/current-selection")
async def get_current_selection(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Get the currently selected file type(s)
    """
    if not session_service.session_exists(session_id):
        raise HTTPException(
            status_code=404,
            detail="No session found"
        )
    
    session = session_service.get_session(session_id)
    selected_types = session.get('selected_types', [])
    
    if not selected_types:
        return {"selected_types": [], "message": "No file types selected yet"}
    
    return {"selected_types": selected_types}

@router.get("/debug-session")
async def debug_session(session_id: str = Query(default=CURRENT_SESSION_ID)):
    """
    Debug endpoint to check session contents
    """
    if not session_service.session_exists(session_id):
        return {
            "exists": False,
            "message": "Session not found"
        }
    
    session_data = session_service.get_session(session_id)
    
    return {
        "exists": True,
        "has_file_categories": 'file_categories' in session_data,
        "file_categories_keys": list(session_data.get('file_categories', {}).keys()) if 'file_categories' in session_data else [],
        "file_counts": {
            cat: len(files) 
            for cat, files in session_data.get('file_categories', {}).items()
        } if 'file_categories' in session_data else {},
        "selected_types": session_data.get('selected_types', []),
        "extraction_path": session_data.get('extraction_path', None),
        "processed_files_dir": PROCESSED_FILES_DIR,
        "has_transaction_data": 'transaction_data' in session_data,
        "has_source_files": 'source_files' in session_data,
        "source_file_count": len(session_data.get('source_files', []))
    }

@router.post("/visualize-individual-transaction-flow")
async def visualize_individual_transaction_flow(
    request: TransactionVisualizationRequest,
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
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
        print(f"🔍 Visualizing flow for transaction: {transaction_id}")
        
        # Check if session exists
        if not session_service.session_exists(session_id):
            raise HTTPException(
                status_code=404,
                detail="No processed ZIP found. Please upload a ZIP file first."
            )
        
        # Get session data
        session_data = session_service.get_session(session_id)
        
        # Get transaction data from session
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            raise HTTPException(
                status_code=400,
                detail="No transaction data available. Please analyze customer journals first."
            )
        
        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)
        
        # Check if transaction exists
        txn_exists = len(df[df['Transaction ID'] == transaction_id]) > 0
        if not txn_exists:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found."
            )
        
        # Get transaction details
        txn_data = df[df['Transaction ID'] == transaction_id].iloc[0]
        
        print(f"✓ Found transaction: {transaction_id}")
        
        # Extract UI flow
        ui_flow_screens = ["No flow data"]
        has_flow = False
        
        # Get file categories from session
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        
        print(f"📂 Found {len(ui_journals)} UI journal file(s)")
        
        if ui_journals:
            try:
                # Get the source file of the transaction to match with correct UI journal
                txn_source_file = str(txn_data.get('Source File', ''))
                print(f"📂 Transaction source file: {txn_source_file}")
                
                # Try to find matching UI journal first
                matching_ui_journal = None
                for ui_journal in ui_journals:
                    ui_journal_name = Path(ui_journal).stem
                    if ui_journal_name == txn_source_file:
                        matching_ui_journal = ui_journal
                        print(f"✓ Found matching UI journal: {ui_journal_name}")
                        break
                
                # If no exact match, try all UI journals
                ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals
                
                for ui_journal_path in ui_journals_to_check:
                    print(f"📖 Parsing UI journal: {ui_journal_path}")
                    
                    ui_df = parse_ui_journal(ui_journal_path)
                    
                    if not ui_df.empty:
                        print(f"✓ Parsed {len(ui_df)} UI events")
                        
                        # Create processor
                        processor = UIJournalProcessor(ui_journal_path)
                        processor.df = ui_df
                        
                        # Convert times
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
                            print(f"⏰ Time range: {start_time} to {end_time}")
                            ui_flow_screens = processor.get_screen_flow(start_time, end_time)
                            
                            if ui_flow_screens and len(ui_flow_screens) > 0:
                                has_flow = True
                                print(f"✓ Flow extracted: {len(ui_flow_screens)} screens from {Path(ui_journal_path).stem}")
                                break  # Found the flow, stop checking other files
                            else:
                                print(f"⚠️ No screens found in time range for {Path(ui_journal_path).stem}")
                        else:
                            print(f"⚠️ Invalid time range")
                    else:
                        print(f"⚠️ Empty UI journal: {Path(ui_journal_path).stem}")
                        
            except Exception as e:
                print(f"❌ Error extracting UI flow: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print("⚠️ No UI journal files available")
        
        # Build response
        response_data = {
            "transaction_id": transaction_id,
            "transaction_type": str(txn_data.get('Transaction Type', 'Unknown')),
            "start_time": str(txn_data.get('Start Time', '')),
            "end_time": str(txn_data.get('End Time', '')),
            "end_state": str(txn_data.get('End State', 'Unknown')),
            "transaction_log": str(txn_data.get('Transaction Log', '')),
            "source_file": str(txn_data.get('Source File', 'Unknown')),  # Include source file
            "ui_flow": ui_flow_screens,
            "has_flow": has_flow,
            "num_events": len(ui_flow_screens) if ui_flow_screens else 0
        }
        
        print(f"✅ Visualization data prepared")
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Visualization failed: {str(e)}"
        )
    
@router.post("/generate-consolidated-flow")
async def generate_consolidated_flow(
    source_file: str = Body(...),
    transaction_type: str = Body(...),
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Generate consolidated flow visualization for all transactions of a specific type
    from a specific source file
    """
    try:
        print(f"🔄 Generating consolidated flow for {transaction_type} from {source_file}")
        
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
        
        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)
        
        # Filter by source file and transaction type
        filtered_df = df[
            (df['Source File'] == source_file) & 
            (df['Transaction Type'] == transaction_type)
        ]
        
        if len(filtered_df) == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No transactions of type '{transaction_type}' found in source '{source_file}'"
            )
        
        print(f"✓ Found {len(filtered_df)} transactions")
        
        # Get UI journal
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        
        # Find matching UI journal
        matching_ui_journal = None
        for ui_journal in ui_journals:
            if Path(ui_journal).stem == source_file:
                matching_ui_journal = ui_journal
                break
        
        if not matching_ui_journal:
            raise HTTPException(
                status_code=404,
                detail=f"No matching UI journal found for source '{source_file}'"
            )
        
        print(f"✓ Found matching UI journal: {matching_ui_journal}")
        
        # Parse UI journal
        ui_df = parse_ui_journal(matching_ui_journal)
        
        if ui_df.empty:
            raise HTTPException(
                status_code=400,
                detail="UI journal is empty or could not be parsed"
            )
        
        print(f"✓ Parsed UI journal with {len(ui_df)} events")
        
        # Create processor
        processor = UIJournalProcessor(matching_ui_journal)
        processor.df = ui_df
        
        # Extract flows for all transactions
        transaction_flows = {}
        all_screens = set()
        transitions = defaultdict(int)
        screen_transactions = defaultdict(list)
        
        for _, txn in filtered_df.iterrows():
            txn_id = txn['Transaction ID']
            
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
            
            if start_time and end_time:
                screens = processor.get_screen_flow(start_time, end_time)
                
                if screens and len(screens) > 0:
                    transaction_flows[txn_id] = {
                        'screens': screens,
                        'start_time': str(start_time),
                        'end_time': str(end_time),
                        'state': txn['End State']
                    }
                    
                    # Track screens and transitions
                    for screen in screens:
                        all_screens.add(screen)
                        screen_transactions[screen].append({
                            'txn_id': txn_id,
                            'start_time': str(start_time),
                            'state': txn['End State']
                        })
                    
                    # Track transitions
                    for i in range(len(screens) - 1):
                        transitions[(screens[i], screens[i + 1])] += 1
        
        if not transaction_flows:
            raise HTTPException(
                status_code=404,
                detail="No UI flow data could be extracted for these transactions"
            )
        
        print(f"✓ Extracted flows for {len(transaction_flows)} transactions")
        print(f"✓ Found {len(all_screens)} unique screens")
        print(f"✓ Found {len(transitions)} unique transitions")
        
        # Prepare response
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
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
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
async def analyze_transaction_llm(
    request: TransactionAnalysisRequest,
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Analyze a transaction log using LLM (Ollama) for anomaly detection
    """
    try:
        transaction_id = request.transaction_id
        print(f"🤖 Analyzing transaction with LLM: {transaction_id}")
        
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
        
        # Convert to DataFrame
        df = pd.DataFrame(transaction_data)
        
        # Find the transaction
        if transaction_id not in df['Transaction ID'].values:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found"
            )
        
        txn_data = df[df['Transaction ID'] == transaction_id].iloc[0]
        transaction_log = str(txn_data.get('Transaction Log', ''))
        
        if not transaction_log:
            raise HTTPException(
                status_code=400,
                detail="No transaction log available for this transaction"
            )
        
        print(f"✓ Found transaction log ({len(transaction_log)} characters)")
        
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
            
            print("🤖 Calling Ollama model...")
            response = ollama.chat(model="llama3_log_analyzer", messages=messages)
            raw_response = response['message']['content'].strip()
            print(f"✓ LLM analysis complete ({len(raw_response)} characters)")
            
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
            
            return structured_response
            
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="Ollama is not installed. Please install it with: pip install ollama"
            )
        except Exception as e:
            print(f"❌ LLM analysis error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"LLM analysis failed: {str(e)}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
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
async def submit_llm_feedback(
    feedback: FeedbackSubmission,
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Submit feedback for LLM analysis
    """
    try:
        print(f"📝 Submitting feedback for transaction: {feedback.transaction_id}")
        
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
            print(f"✓ Feedback saved to file")
        except Exception as e:
            print(f"⚠️ Could not save to file: {e}")
        
        # Also store in session for immediate retrieval
        if not session_service.session_exists(session_id):
            session_service.create_session(session_id)
        
        session_data = session_service.get_session(session_id)
        
        if 'feedback_data' not in session_data:
            session_data['feedback_data'] = []
        
        session_data['feedback_data'].append(feedback_record)
        session_service.update_session(session_id, session_data)
        
        print(f"✓ Feedback stored in session")
        
        return {
            "status": "success",
            "message": f"Thank you {feedback.user_name}! Your feedback has been recorded.",
            "timestamp": feedback_record['timestamp']
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit feedback: {str(e)}"
        )


@router.get("/get-feedback/{transaction_id}")
async def get_feedback(
    transaction_id: str,
    session_id: str = Query(default=CURRENT_SESSION_ID)
):
    """
    Get all feedback for a specific transaction
    """
    try:
        print(f"📖 Retrieving feedback for transaction: {transaction_id}")
        
        all_feedback = []
        
        # Get from session
        if session_service.session_exists(session_id):
            session_data = session_service.get_session(session_id)
            session_feedback = session_data.get('feedback_data', [])
            
            # Filter by transaction ID
            all_feedback.extend([
                f for f in session_feedback 
                if f.get('transaction_id') == transaction_id
            ])
        
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
            except Exception as e:
                print(f"⚠️ Could not read feedback file: {e}")
        
        print(f"✓ Found {len(all_feedback)} feedback record(s)")
        
        return {
            "transaction_id": transaction_id,
            "feedback_count": len(all_feedback),
            "feedback": all_feedback
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve feedback: {str(e)}"
        )


