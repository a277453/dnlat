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
import re

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
                
                # Only add source if it has transactions
                source_files.append(source_filename)
                
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
        
        # Store in session (remove duplicates from source_files)
        unique_source_files = list(set(source_files))
        unique_source_files.sort()
        
        session_service.update_session(session_id, 'transaction_data', transaction_records)
        session_service.update_session(session_id, 'source_files', unique_source_files)
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
        
        # Remove duplicates - keep only unique source files
        source_files = list(set(source_files))
        source_files.sort()
        
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
        # Generate statistics by transaction type
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
    Compare UI flows of two transactions with durations
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
        
        # Extract UI flows for both transactions WITH DURATIONS
        ui_flow_1 = ["No screens in time range"]
        ui_flow_2 = ["No screens in time range"]
        
        if ui_journals:
            try:
                txn1_source_file = str(txn1_data.get('Source File', ''))
                txn2_source_file = str(txn2_data.get('Source File', ''))
                
                print(f"📂 Transaction 1 source: {txn1_source_file}")
                print(f"📂 Transaction 2 source: {txn2_source_file}")
                
                # Enhanced function to extract flow WITH durations
                def extract_flow_with_durations(txn_data, txn_source_file, txn_label):
                    flow_screens = ["No screens in time range"]
                    
                    # Try to find matching UI journal
                    matching_ui_journal = None
                    for ui_journal in ui_journals:
                        ui_journal_name = Path(ui_journal).stem
                        if ui_journal_name == txn_source_file:
                            matching_ui_journal = ui_journal
                            print(f"✓ Found matching UI journal for {txn_label}: {ui_journal_name}")
                            break
                    
                    ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals
                    
                    for ui_journal_path in ui_journals_to_check:
                        print(f"📖 Parsing UI journal for {txn_label}: {ui_journal_path}")
                        
                        ui_df = parse_ui_journal(ui_journal_path)
                        
                        if not ui_df.empty:
                            print(f"✓ Parsed {len(ui_df)} UI events for {txn_label}")
                            
                            processor = UIJournalProcessor(ui_journal_path)
                            processor.df = ui_df
                            
                            # Parse times
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
                            
                            start_time = parse_time_local(txn_data['Start Time'])
                            end_time = parse_time_local(txn_data['End Time'])
                            
                            if start_time and end_time:
                                print(f"⏰ {txn_label} time range: {start_time} to {end_time}")
                                
                                # Get unique screens
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
                                                    print(f"✓ {txn_label} flow with durations: {len(flow_details)} screens")
                                                    break
                                            else:
                                                # No durations, use simple screens
                                                flow_screens = unique_screens
                                                print(f"⚠️ No UI events in time range, using simple screens for {txn_label}")
                                                break
                                        else:
                                            # No columns found, use simple screens
                                            flow_screens = unique_screens
                                            print(f"⚠️ Columns not found, using simple screens for {txn_label}")
                                            break
                                    except Exception as e:
                                        print(f"⚠️ Could not add durations for {txn_label}: {e}")
                                        flow_screens = unique_screens
                                        break
                                else:
                                    print(f"⚠️ No screens found for {txn_label}")
                        else:
                            print(f"⚠️ Empty UI journal for {txn_label}")
                    
                    return flow_screens
                
                # Extract flows for both transactions
                ui_flow_1 = extract_flow_with_durations(txn1_data, txn1_source_file, "Transaction 1")
                ui_flow_2 = extract_flow_with_durations(txn2_data, txn2_source_file, "Transaction 2")
                
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
    Generate UI flow visualization for a single transaction with durations
    Shows unique screens with correct durations
    """
    try:
        transaction_id = request.transaction_id
        print(f"🔍 Visualizing flow for transaction: {transaction_id}")
        
        if not session_service.session_exists(session_id):
            raise HTTPException(status_code=404, detail="No processed ZIP found.")
        
        session_data = session_service.get_session(session_id)
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            raise HTTPException(status_code=400, detail="No transaction data available.")
        
        df = pd.DataFrame(transaction_data)
        
        if transaction_id not in df['Transaction ID'].values:
            raise HTTPException(status_code=404, detail=f"Transaction {transaction_id} not found.")
        
        txn_data = df[df['Transaction ID'] == transaction_id].iloc[0]
        print(f"✓ Found transaction: {transaction_id}")
        
        ui_flow_screens = ["No flow data"]
        has_flow = False
        
        file_categories = session_data.get('file_categories', {})
        ui_journals = file_categories.get('ui_journals', [])
        print(f"📂 Found {len(ui_journals)} UI journal file(s)")
        
        if ui_journals:
            try:
                txn_source_file = str(txn_data.get('Source File', ''))
                print(f"📂 Transaction source file: {txn_source_file}")
                
                matching_ui_journal = None
                for ui_journal in ui_journals:
                    ui_journal_name = Path(ui_journal).stem
                    if ui_journal_name == txn_source_file:
                        matching_ui_journal = ui_journal
                        print(f"✓ Found matching UI journal: {ui_journal_name}")
                        break
                
                ui_journals_to_check = [matching_ui_journal] if matching_ui_journal else ui_journals
                
                for ui_journal_path in ui_journals_to_check:
                    print(f"📖 Parsing UI journal: {ui_journal_path}")
                    
                    ui_df = parse_ui_journal(ui_journal_path)
                    
                    if not ui_df.empty:
                        print(f"✓ Parsed {len(ui_df)} UI events")
                        
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
                            print(f"⏰ Time range: {start_time} to {end_time}")
                            
                            try:
                                print("🔄 Extracting flow with durations...")
                                
                                # Get unique screen list (from processor)
                                unique_screens = processor.get_screen_flow(start_time, end_time)
                                
                                if not unique_screens or len(unique_screens) == 0:
                                    print("⚠️ No screens found in time range")
                                    continue
                                
                                print(f"✓ Found {len(unique_screens)} unique screens")
                                
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
                                
                                print(f"✓ Using columns: time='{time_col}', screen='{screen_col}'")
                                
                                # Ensure time column is in time format
                                if ui_df[time_col].dtype == 'object' or str(ui_df[time_col].dtype).startswith('datetime'):
                                    ui_df[time_col] = pd.to_datetime(ui_df[time_col], errors='coerce').dt.time
                                
                                # Get ALL screen events in chronological order
                                ui_filtered = ui_df[
                                    (ui_df[time_col] >= start_time) & 
                                    (ui_df[time_col] <= end_time)
                                ].copy()
                                
                                print(f"✓ Filtered {len(ui_filtered)} UI events in time range")
                                
                                if len(ui_filtered) > 0:
                                    # Build complete sequence with all occurrences
                                    all_events = []
                                    for idx, row in ui_filtered.iterrows():
                                        screen = str(row.get(screen_col, ''))
                                        time_val = row.get(time_col)
                                        
                                        if screen and not pd.isna(screen):
                                            all_events.append((screen, time_val))
                                    
                                    print(f"✓ Built sequence of {len(all_events)} screen events")
                                    
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
                                    
                                    print(f"✓ Mapped {len(screen_info)} unique screens to time ranges")
                                    
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
                                        print(f"✅ Created detailed flow: {len(ui_flow_details)} unique screens, {with_duration} with durations")
                                        
                                        # Debug: print all screens
                                        for i, screen in enumerate(ui_flow_details):
                                            dur_str = f"{screen['duration']:.1f}s" if screen['duration'] is not None else "N/A"
                                            print(f"   {i+1}. {screen['screen']} @ {screen['timestamp']} ({dur_str})")
                                        
                                        break
                                    else:
                                        raise Exception("No screens after processing")
                                else:
                                    raise Exception("No filtered events")
                                    
                            except Exception as e:
                                print(f"❌ Enhancement failed: {e}")
                                import traceback
                                traceback.print_exc()
                                
                                # Fallback
                                try:
                                    simple_screens = processor.get_screen_flow(start_time, end_time)
                                    if simple_screens and len(simple_screens) > 0:
                                        ui_flow_screens = simple_screens
                                        has_flow = True
                                        print(f"✅ Using fallback: {len(simple_screens)} screens")
                                        break
                                except:
                                    continue
                        else:
                            print(f"⚠️ Invalid time range")
                    else:
                        print(f"⚠️ Empty UI journal")
                        
            except Exception as e:
                print(f"❌ Error: {str(e)}")
                import traceback
                traceback.print_exc()
        
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
        
        print(f"✅ Response prepared:")
        print(f"   - has_flow: {response_data['has_flow']}")
        print(f"   - has_details: {response_data['has_details']}")
        print(f"   - num_events: {response_data['num_events']}")
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Visualization failed: {str(e)}")
    
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
    Parse counter data from TRC trace - simple space-separated format
    No separator logic - just header detection and column alignment
    """
    counter_rows = []
    
    # Find header line
    header_line = None
    header_idx = -1
    
    for idx, line in enumerate(log_lines):
        stripped = line.strip()
        # Header line contains "No Ty ID" or "No Ty" and column names
        if 'No' in stripped and 'Ty' in stripped and ('ID' in stripped or 'UnitName' in stripped):
            header_line = line
            header_idx = idx
            # print(f"    Found header at line {idx}: {stripped[:80]}")
            break
    
    if not header_line or header_idx == -1:
        print("    ⚠️ No header line found")
        return []
    
    # Parse data lines after header
    for idx in range(header_idx + 1, len(log_lines)):
        line = log_lines[idx]
        
        # DEBUG: Print each line being examined
        # print(f"    Examining line {idx}: {line[:100]}")
        
        # Skip empty lines, CCdm lines, or separator lines
        if (not line.strip() or 
            'CCdm' in line or 
            'usTellerID' in line or
            line.strip().startswith('*')):
            # print(f"      -> Skipped (empty/CCdm/separator)")
            continue
        
        # Skip lines that start with whitespace (continuation lines)
        if line.startswith(' ') or line.startswith('\t'):
           #  print(f"      -> Skipped (continuation line)")
            continue
        
        # Split by whitespace
        parts = line.split()
        
        if len(parts) < 3:
           # print(f"      -> Skipped (less than 3 parts)")
            continue
        
        # Check if this is a valid counter data line
        # Valid lines start with: No Ty ID (e.g., "03 04 95829")
        # First field should be cassette number (01-50)
        if not parts[0].isdigit() or int(parts[0]) > 50:
           # print(f"      -> Skipped (invalid cassette number: {parts[0]})")
            continue
        
        # Second field should be type number (01-20 typically)
        if not parts[1].isdigit():
           #  print(f"      -> Skipped (invalid type: {parts[1]})")
            continue
        
        # Third field should be ID (numeric)
        if not parts[2].isdigit():
           #  print(f"      -> Skipped (invalid ID: {parts[2]})")
            continue
        
        # print(f"      -> ✓ PARSING THIS LINE")
        
        try:
            counter_data = {}
            
            # Parse fields: No Ty ID UnitName Cur Val Init Actn Rej Safe Min Max AppL DevL Status HWsens
            counter_data['No'] = parts[0] if len(parts) > 0 else ''
            counter_data['Ty'] = parts[1] if len(parts) > 1 else ''
            counter_data['ID'] = parts[2] if len(parts) > 2 else ''
            counter_data['UnitName'] = parts[3] if len(parts) > 3 else ''
            
            idx_part = 4
            
            # Check for currency
            if idx_part < len(parts) and parts[idx_part] in ['EUR', 'INR', 'USD', 'GBP', 'JPY', 'CNY']:
                counter_data['Cur'] = parts[idx_part]
                idx_part += 1
            else:
                counter_data['Cur'] = ''
            
            # Parse numeric fields
            field_mapping = [
                ('Val', 'Val'),
                ('Init', 'Ini'),
                ('Actn', 'Cnt'),
                ('Rej', 'RCnt'),
                ('Safe', 'Safe'),
                ('Min', 'Min'),
                ('Max', 'Max')
            ]
            
            for _, target_name in field_mapping:
                if idx_part < len(parts):
                    val = parts[idx_part]
                    counter_data[target_name] = val
                    idx_part += 1
                else:
                    counter_data[target_name] = ''
            
            counter_data['Disp'] = ''
            counter_data['Pres'] = ''
            counter_data['Retr'] = ''
            
            counter_data['A'] = parts[idx_part] if idx_part < len(parts) else ''
            idx_part += 1
            
            if idx_part < len(parts):
                idx_part += 1
            
            counter_data['St'] = parts[idx_part] if idx_part < len(parts) else ''
            
            counter_data['Record_Type'] = 'Logical'
            counter_rows.append(counter_data)
            
            # print(f"      -> Added counter row: No={counter_data['No']}, UnitName={counter_data['UnitName']}")
            
        except Exception as e:
           # print(f"      -> ⚠️ Error parsing: {str(e)[:50]}")
            continue
    
   # print(f"    Total rows parsed: {len(counter_rows)}")
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
    Extract ALL counter blocks from TRC trace file
    Returns list of dicts with 'time', 'timestamp', 'data'
    CRITICAL: Each CCdmCashUnitInfoDataEx block is kept SEPARATE - NO MERGING
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
    Get list of source files that have matching TRC trace files
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
        
        print(f"✓ Found {len(matching_sources)} sources with matching TRC trace files")
        
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
    Get counter data from TRC Trace files mapped to transaction timestamp
    """
    try:
        print(f"📊 Getting counter data for transaction: {request.transaction_id}")
        
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
        if request.transaction_id not in df['Transaction ID'].values:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {request.transaction_id} not found"
            )
        
        txn_data = df[df['Transaction ID'] == request.transaction_id].iloc[0]
        
        # Get TRC trace files
        file_categories = session_data.get('file_categories', {})
        trc_trace_files = file_categories.get('trc_trace', [])
        
        if not trc_trace_files:
            raise HTTPException(
                status_code=400,
                detail="No TRC trace files available"
            )
        
        print(f"✓ Found {len(trc_trace_files)} TRC trace file(s)")
        
        # Parse transaction date from source file (format: YYYYMMDD -> YYMMDD)
        txn_date_full = request.source_file  # e.g., "20250404"
        txn_date_short = txn_date_full[2:] if len(txn_date_full) == 8 else txn_date_full  # "250404"
        
        print(f"✓ Transaction date: {txn_date_full} (searching for {txn_date_short})")
        
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
                    print(f"✓ Found matching TRC trace: {Path(trc_file).name}")
                    break
            except Exception as e:
                print(f"⚠️ Error reading {trc_file}: {e}")
                continue
        
        if not matching_trc:
            raise HTTPException(
                status_code=404,
                detail=f"No matching TRC trace file found for date '{txn_date_full}' (searched for '{txn_date_short}')"
            )
        
        print(f"✓ Found matching TRC trace: {matching_trc}")
        
        # Extract counter blocks from TRC trace file
        txn_start_time = str(txn_data.get('Start Time', ''))
        txn_end_time = str(txn_data.get('End Time', ''))

        print(f"📊 Transaction times: {txn_start_time} to {txn_end_time}")

        # OPTIMIZATION: Extract ALL counter blocks from TRC file ONCE
        all_counter_blocks = extract_counter_blocks(matching_trc)  # ← CHANGED: Only 1 parameter

        if not all_counter_blocks:
            print("⚠️ No counter blocks found")
            first_counter_data = []
            last_counter_data = []
            first_timestamp = txn_start_time
            last_timestamp = txn_end_time
        else:
            # Simply use first and last blocks from the file
            first_block = all_counter_blocks[0]
            last_block = all_counter_blocks[-1]
            
            first_counter_data = first_block['data']
            last_counter_data = last_block['data']
            first_timestamp = first_block['timestamp']
            last_timestamp = last_block['timestamp']
            
            print(f"✓ First counter: {len(first_counter_data)} rows at {first_timestamp}")
            print(f"✓ Last counter: {len(last_counter_data)} rows at {last_timestamp}")
        
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
        
        # Get all transactions from selected transaction to end
        selected_txn_index = df[df['Transaction ID'] == request.transaction_id].index[0]
        transactions_subset = df[df.index >= selected_txn_index]
        
        print(f"✓ Building counter per transaction table for {len(transactions_subset)} transactions")
        
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
            # Pattern: "Dispense info - 1 note(s) of 500,00 INR from cassette 5 (SLOT3)"
            count_info = []
            
            for log_line in txn_log.split('\n'):
                # Look for dispense info pattern
                match = re.search(r'(\d+)\s+note\(s\)\s+of\s+([\d,\.]+)\s+([A-Z]{3})', log_line, re.IGNORECASE)
                if match:
                    note_count = match.group(1)
                    amount = match.group(2).replace(',', '.')  # Handle comma as decimal separator
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
                print(f"⚠️ Error checking counters for {txn_id}: {e}")
            
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
        
        print(f"✓ Created counter per transaction table with {len(counter_per_transaction)} entries")
        
        response_data = {
            "transaction_id": request.transaction_id,
            "source_file": request.source_file,
            "all_blocks": all_counter_blocks,
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