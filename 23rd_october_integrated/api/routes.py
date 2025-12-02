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
    CategoryCount,
    TransactionVisualizationRequest
)
from pathlib import Path
from typing import Dict, List
import shutil
from fastapi import Body
import os
import pandas as pd
from modules.ui_journal_processor import UIJournalProcessor, parse_ui_journal
from datetime import datetime
from collections import defaultdict

router = APIRouter()

# Simple session ID for now (use UUID in production)
CURRENT_SESSION_ID = "current_session"

# Global variable to track processed files directory (for registry endpoints)
PROCESSED_FILES_DIR = None

def set_processed_files_dir(directory: str):
    """Set the directory where processed files are stored"""
    global PROCESSED_FILES_DIR
    PROCESSED_FILES_DIR = directory
    print(f"✓ Processed files directory set to: {directory}")

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
                    print(f"  📁 Moved {source.name} to {category}/")
                except Exception as e:
                    print(f"  ❌ Failed to move {source.name}: {e}")
                    continue
        
        organized_categories[category] = organized_files
    
    return organized_categories

@router.post("/process-zip", response_model=FileCategorizationResponse)
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
        # Read the uploaded file
        zip_content = await file.read()
        
        # Step 2: Extract
        extraction_service = ZipExtractionService()
        extract_path = extraction_service.extract_zip(zip_content)
        
        # Step 3: Categorize
        categorization_service = CategorizationService()
        file_categories = categorization_service.categorize_files(extract_path)
        
        # Step 3.5: PHYSICALLY ORGANIZE FILES INTO SUBDIRECTORIES
        print(f"🔧 Organizing files into subdirectories...")
        file_categories = organize_files_into_subdirectories(extract_path, file_categories)
        
        # Set processed files directory for registry endpoints
        set_processed_files_dir(str(extract_path))
        
        # Debug output
        print(f"🔍 DEBUG: About to create session")
        print(f"📁 File categories: {list(file_categories.keys())}")
        print(f"📊 File counts: {dict((k, len(v)) for k, v in file_categories.items())}")
        
        # Step 4: Store in session
        session_service.create_session(CURRENT_SESSION_ID, file_categories, extract_path)
        
        # Debug output
        print(f"✅ DEBUG: Session created successfully")
        print(f"🔍 DEBUG: Verifying session exists: {session_service.session_exists(CURRENT_SESSION_ID)}")
        
        # Step 5: Process and return results
        processing_service = ProcessingService()
        result = processing_service.prepare_response(file_categories, extract_path)
        
        return result
        
    except Exception as e:
        print(f"❌ ERROR in process_zip: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing ZIP file: {str(e)}"
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
        import json
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
    
