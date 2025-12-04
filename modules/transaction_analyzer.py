"""
Transaction Analyzer Service - Parses and analyzes customer journal files
"""

import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from .configManager import xml_to_dict
import os
from datetime import datetime, time
from modules.logging_config import logger


logger.info("Transaction Analyzer Service loaded")

            
class TransactionAnalyzerService:
    """
    Service for parsing and analyzing customer journal transactions
    """
    
    def __init__(self):
        # Load configuration when service is initialized
        # Define paths relative to this file's location for portability
        base_dir = Path(__file__).resolve().parent
        possible_paths = [
            base_dir / 'config' / 'dnLogAtConfig.xml',        # e.g., project/modules/config/dnLogAtConfig.xml
            base_dir.parent / 'config' / 'dnLogAtConfig.xml', # e.g., project/config/dnLogAtConfig.xml
            base_dir / 'dnLogAtConfig.xml',                   # e.g., project/dnLogAtConfig.xml
        ]
        
        config_path = None
        for path in possible_paths:
            if path.exists():
                config_path = path
                break
        
        if config_path is None:
            logger.error(
                "dnLogAtConfig.xml not found. Searched in:\n%s", 
                "\n".join(map(str, possible_paths))
            )
            raise FileNotFoundError(
                "dnLogAtConfig.xml not found. Please ensure the config file exists. Searched in:\n" +
                "\n".join(map(str, possible_paths))
            )
        
        self.real_dict, self.start_key, self.end_key, self.chain_key = xml_to_dict(config_path)
        logger.info("Configuration loaded successfully from %s", config_path)
    
    # ============================================
    # NEW METHOD - ADDED TO FIX THE ERROR
    # ============================================
    
    def analyze_customer_journals(self, customer_journal_files: List[str]) -> Dict:
        """
        Analyze customer journal files and return transaction data
        
        This method processes multiple customer journal files and aggregates
        all transactions into a single response.
        
        Args:
            customer_journal_files: List of paths to customer journal files
        Returns:
            Dictionary containing:
            - transactions: List of transaction dictionaries
            - summary: Summary statistics
        """
        all_transactions = []
        
        for journal_file in customer_journal_files:
            try:
                logger.info("Processing: %s", Path(journal_file).name)
                
                # Use the existing parse_customer_journal method
                df = self.parse_customer_journal(journal_file)
                
                if df is None or df.empty:
                    logger.warning("No data from %s", Path(journal_file).name)
                    continue
                
                # Convert DataFrame to list of dictionaries
                transactions = df.to_dict('records')
                
                # Convert any Timestamp objects to strings for JSON serialization
                for txn in transactions:
                    for key, value in list(txn.items()):
                        if pd.isna(value):
                            txn[key] = None
                        elif hasattr(value, 'strftime'):
                            # Convert datetime/time to string
                            if hasattr(value, 'time'):
                                txn[key] = value.time().strftime('%H:%M:%S')
                            else:
                                txn[key] = value.strftime('%H:%M:%S')
                        elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                            txn[key] = str(value)
                
                all_transactions.extend(transactions)
                logger.info("Found %d transactions in %s", len(transactions), Path(journal_file).name)
                
            except Exception as e:
                logger.error("Error processing %s: %s", Path(journal_file).name, e, exc_info=True)
                continue
        
        if not all_transactions:
            logger.warning("No transactions found in any journal files")
            return {
                "transactions": [],
                "summary": {
                    "total_transactions": 0,
                    "successful": 0,
                    "unsuccessful": 0,
                    "transaction_types": []
                }
            }
        
        # Generate summary statistics
        df_all = pd.DataFrame(all_transactions)
        
        summary = {
            "total_transactions": len(all_transactions),
            "successful": 0,
            "unsuccessful": 0,
            "transaction_types": []
        }
        
        # Calculate success/failure counts
        if 'End State' in df_all.columns:
            summary["successful"] = len(df_all[df_all['End State'] == 'Successful'])
            summary["unsuccessful"] = len(df_all[df_all['End State'] == 'Unsuccessful'])
        
        # Get unique transaction types
        if 'Transaction Type' in df_all.columns:
            summary["transaction_types"] = df_all['Transaction Type'].dropna().unique().tolist()
        
        logger.info("Analysis complete: %d total transactions", summary['total_transactions'])
        logger.info("Successful: %d, Unsuccessful: %d", summary['successful'], summary['unsuccessful'])
        
        return {
            "transactions": all_transactions,
            "summary": summary
        }
    
    # ============================================
    # ALL YOUR EXISTING METHODS BELOW (UNCHANGED)
    # ============================================
    
    def parse_customer_journal(self, file_path: str) -> pd.DataFrame:
        """
        Parse a customer journal file and return DataFrame with transactions
        
        Args:
            file_path: Path to the customer journal file
        Returns:
            DataFrame with transaction data
        """
        dummy = Path(file_path).stem
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            logger.error("Failed to read file %s: %s", file_path, e, exc_info=True)
            return pd.DataFrame()
        
        # Parse all lines first
        parsed_rows = []
        for line in lines:
            line = line.strip()
            if not line or set(line) <= {'*'}:
                continue
            match = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(\d+)\s*(.*)", line)
            if match:
                timestamp_str, tid, message = match.groups()
                try:
                    timestamp = datetime.strptime(timestamp_str, "%H:%M:%S").time()
                except ValueError:
                    timestamp = None
                parsed_rows.append([timestamp, tid, message])
            else:
                parsed_rows.append([None, None, line])

        df = pd.DataFrame(parsed_rows, columns=["timestamp", "tid", "message"])
        
        transactions = self._find_all_transactions(df, dummy)
        
        logger.debug("Parsed %d transactions from %s", len(transactions), file_path)
        return pd.DataFrame(transactions)
    
    def _find_all_transactions(self, df: pd.DataFrame, dummy: str) -> List[Dict]:
        """Find all individual transactions in the parsed data"""
        logger.info("Finding all transactions in parsed data for %s", dummy)
        transactions_bounds = []
        i = 0
        
        while i < len(df):
            row = df.iloc[i]
            
            # Look for transaction start OR transaction chaining
            if (str(row["tid"]) in [str(tid) for tid in self.start_key] or 
                str(row["tid"]) in [str(tid) for tid in self.chain_key]):
                start_idx = i
                j = i + 1
                end_idx = None
                
                # Look for the corresponding end
                while j < len(df):
                    current_row = df.iloc[j]
                    
                    # Found an end TID
                    if str(current_row["tid"]) in [str(tid) for tid in self.end_key]:
                        end_idx = j
                        break
                    
                    # If we encounter another start or chaining, break
                    if ((str(current_row["tid"]) in [str(tid) for tid in self.start_key] or 
                        str(current_row["tid"]) in [str(tid) for tid in self.chain_key]) and j > i + 3):
                        break
                    
                    j += 1
                
                if end_idx is not None:
                    transactions_bounds.append((start_idx, end_idx))
                    i = end_idx + 1
                else:
                    i += 1
            else:
                i += 1
        
        transactions = []
        
        for start_idx, end_idx in transactions_bounds:
            txn_segment = df.iloc[start_idx:end_idx+1]
            
            # Find start details
            start_time = None
            txn_id = None
            matched_start_tid = None
            
            # Check for regular start
            for start_tid in self.start_key:
                start_matches = txn_segment[txn_segment["tid"] == str(start_tid)]
                if not start_matches.empty:
                    start_row = start_matches.iloc[0]
                    start_time = start_row["timestamp"]
                    match = re.search(r"Transaction no\. '([^']*)'", start_row["message"])
                    txn_id = match.group(1) if match and match.group(1).strip() else (dummy + start_time.strftime("%H%M%S"))
                    matched_start_tid = start_tid
                    break

            # If no regular start found, check for transaction chaining
            if start_time is None:
                for chain_tid in self.chain_key:
                    chain_matches = txn_segment[txn_segment["tid"] == str(chain_tid)]
                    if not chain_matches.empty:
                        start_row = chain_matches.iloc[0]
                        start_time = start_row["timestamp"]
                        txn_id = dummy + start_time.strftime("%H%M%S") if start_time else f"CHAIN_{dummy}"
                        matched_start_tid = chain_tid
                        break
            
            # Find end details
            end_time = None
            end_state = "Unknown"
            
            for end_tid in self.end_key:
                end_matches = txn_segment[txn_segment["tid"] == str(end_tid)]
                if not end_matches.empty:
                    end_row = end_matches.iloc[-1]
                    end_time = end_row["timestamp"]
                    end_msg = end_row["message"]
                    
                    if ("end-state'N'" in end_msg or "end-state'n'" in end_msg or 
                        "state 'N'" in end_msg or "state 'n'" in end_msg):
                        end_state = "Successful"
                    elif ("end-state'E'" in end_msg or "end-state'e'" in end_msg or 
                        "state 'E'" in end_msg or "state 'e'" in end_msg or 
                        "state 'C'" in end_msg or "state 'c'" in end_msg):
                        end_state = "Unsuccessful"
                    else:
                        end_state = "Unknown"
                    break
            
            # Find transaction type
            txn_type = "Unknown"
            func_matches = txn_segment[txn_segment["tid"] == "3217"]
            if not func_matches.empty:
                for _, func_row in func_matches.iterrows():
                    func_match = re.search(r"Function\s+'([^']+)'", func_row["message"])
                    if func_match:
                        raw_func = func_match.group(1).strip()
                        txn_type = self.real_dict.get(raw_func, raw_func)
                        break
            
            # Collect full transaction log
            txn_log_lines = []
            for _, row in txn_segment.iterrows():
                ts = row["timestamp"].strftime("%H:%M:%S") if row["timestamp"] else "??:??:??"
                tid_val = row["tid"] if row["tid"] else ""
                msg_val = row["message"]
                txn_log_lines.append(f"{ts} {tid_val} {msg_val}")
            
            txn_log = "\n".join(txn_log_lines)
            
            # Calculate duration
            duration_seconds = 0
            if start_time and end_time:
                try:
                    # Combine with a dummy date to calculate timedelta
                    start_dt = datetime.combine(datetime.today(), start_time)
                    end_dt = datetime.combine(datetime.today(), end_time)
                    duration_seconds = (end_dt - start_dt).total_seconds()
                except Exception as e:
                    logger.warning("Duration calculation failed for txn %s: %s", txn_id, e)
                    duration_seconds = 0 # Keep it 0 if calculation fails
            
            transactions.append({
                "Transaction ID": txn_id,
                "Start Time": start_time,
                "End Time": end_time,
                "Duration (seconds)": duration_seconds,
                "Transaction Type": txn_type,
                "End State": end_state,
                "Transaction Log": txn_log,
                "Source_File": dummy
            })
        logger.debug("Found %d transactions for %s", len(transactions), dummy)
        return transactions
    
    def extract_actual_flows_from_txt_file(self, txt_file_path: str, selected_transaction_type: str) -> dict:
        logger.info("Extracting actual flows from %s for type '%s'", txt_file_path, selected_transaction_type)
        """
        Extract the actual flows from the transaction_flows.txt file for a specific transaction type
        """
        flows = {}
        
        if not os.path.exists(txt_file_path):
            logger.warning("File not found: %s", txt_file_path)
            return flows
        
        try:
            with open(txt_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            logger.error("Failed to read txt file %s: %s", txt_file_path, e, exc_info=True)
            return flows
        
        blocks = content.split('-' * 60)
        
        for block in blocks:
            if not block.strip():
                continue
            
            lines = block.strip().split('\n')
            txn_id = None
            txn_type = None
            flow_line = None
            
            for line in lines:
                if line.startswith('Transaction ID:'):
                    txn_id = line.split(':', 1)[1].strip()
                elif line.startswith('Transaction Type:'):
                    txn_type = line.split(':', 1)[1].strip()
                elif line.startswith('Flow:'):
                    flow_line = line.split(':', 1)[1].strip()
            
            if txn_type == selected_transaction_type and txn_id and flow_line:
                if flow_line == 'No screen data available':
                    flows[txn_id] = {'screens': ['No flow data'], 'timestamp': ''}
                else:
                    screens = []
                    for part in flow_line.split('--'):
                        part = part.strip()
                        if '[' in part and ']' in part:
                            screen = part.split('[')[0].strip()
                            if screen:
                                screens.append(screen)
                    flows[txn_id] = {'screens': screens if screens else ['No flow data'], 'timestamp': ''}
        
        logger.info("Extracted %d flows for type '%s'", len(flows), selected_transaction_type)
        return flows
    
    def create_side_by_side_flow_comparison_data(self, df: pd.DataFrame, txn1_id: str, txn2_id: str, txt_file_path: str) -> dict:
        """
        Create side-by-side flow comparison data for two transactions
        Uses Longest Common Subsequence (LCS) algorithm to find sequential matches
        """
        
    
    def generate_data_based_comparison_analysis(self, txn1_data, txn2_data, txn1_id: str, txn2_id: str, flows_data: dict) -> str:
        """
        Generate detailed comparison analysis based purely on transaction data without AI/LLM
        Uses only absolute data from the transactions - no generated or assumed data
        """
        logger.info(f"Starting comparison analysis for transactions {txn1_id} and {txn2_id}")

        analysis = []
        analysis.append("")
        
        # Get flow data
        txn1_flow = flows_data.get(txn1_id, {'screens': ['No flow data']})
        txn2_flow = flows_data.get(txn2_id, {'screens': ['No flow data']})
        txn1_screens = txn1_flow['screens']
        txn2_screens = txn2_flow['screens']
        logger.debug("Transaction 1 screens: %s", txn1_screens)
        logger.debug("Transaction 2 screens: %s", txn2_screens)
        
        # Timing analysis with actual data only
        txn1_duration = None
        txn2_duration = None
        
        try:
            if txn1_data['Start Time'] and txn1_data['End Time']:
                # Handle both time objects and strings
                start_time = txn1_data['Start Time'] if isinstance(txn1_data['Start Time'], time) else datetime.strptime(str(txn1_data['Start Time']), '%H:%M:%S').time()
                end_time = txn1_data['End Time'] if isinstance(txn1_data['End Time'], time) else datetime.strptime(str(txn1_data['End Time']), '%H:%M:%S').time()
                
                txn1_duration = (datetime.combine(datetime.today(), end_time) - 
                            datetime.combine(datetime.today(), start_time)).total_seconds()
                logger.info("Transaction 1 duration calculated: %.1f seconds", txn1_duration)
                
            if txn2_data['Start Time'] and txn2_data['End Time']:
                # Handle both time objects and strings
                start_time = txn2_data['Start Time'] if isinstance(txn2_data['Start Time'], time) else datetime.strptime(str(txn2_data['Start Time']), '%H:%M:%S').time()
                end_time = txn2_data['End Time'] if isinstance(txn2_data['End Time'], time) else datetime.strptime(str(txn2_data['End Time']), '%H:%M:%S').time()
                
                txn2_duration = (datetime.combine(datetime.today(), end_time) - 
                            datetime.combine(datetime.today(), start_time)).total_seconds()
                logger.info("Transaction 2 duration calculated: %.1f seconds", txn2_duration)
            
            if txn1_duration is not None and txn2_duration is not None:
                duration_diff = txn2_duration - txn1_duration
                logger.debug("Duration difference: %.1f seconds", duration_diff)
                analysis.append(f"**Actual Duration Data:**")
                analysis.append(f"   - Transaction 1 Duration: {txn1_duration:.1f} seconds")
                analysis.append(f"   - Transaction 2 Duration: {txn2_duration:.1f} seconds")
                
                if duration_diff > 0:
                    analysis.append(f"   - Transaction 2 took {duration_diff:.1f} seconds longer")
                elif duration_diff < 0:
                    analysis.append(f"   - Transaction 1 took {abs(duration_diff):.1f} seconds longer")
                else:
                    analysis.append(f"   - Both transactions took exactly the same time")
            else:
                logger.warning("One or both transaction durations could not be calculated")
                analysis.append(f"**⏱️ Duration Data:**")
                if txn1_duration is not None:
                    analysis.append(f"   - Transaction 1 Duration: {txn1_duration:.1f} seconds")
                else:
                    analysis.append(f"   - Transaction 1 Duration: Cannot calculate (missing start/end time)")
                
                if txn2_duration is not None:
                    analysis.append(f"   - Transaction 2 Duration: {txn2_duration:.1f} seconds")
                else:
                    analysis.append(f"   - Transaction 2 Duration: Cannot calculate (missing start/end time)")
        
        except Exception as e:
            logger.error("Error calculating durations: %s", e, exc_info=True)
            analysis.append(f"❌ Unable to calculate duration: {str(e)}")
        
        analysis.append("")
        
        # Actual step count analysis
        analysis.append(f"**Actual Step Counts:**")
        analysis.append(f"   - Transaction 1 Steps: {len(txn1_screens)}")
        analysis.append(f"   - Transaction 2 Steps: {len(txn2_screens)}")
        
        step_diff = len(txn2_screens) - len(txn1_screens)
        logger.info("Step count comparison: txn1=%d, txn2=%d, diff=%d", len(txn1_screens), len(txn2_screens), step_diff)
    
        if step_diff > 0:
            analysis.append(f"   - Transaction 2 has {step_diff} more steps")
        elif step_diff < 0:
            analysis.append(f"   - Transaction 1 has {abs(step_diff)} more steps")
        else:
            analysis.append(f"   - Both transactions have identical step counts")
        
        analysis.append("")
        
        # Screen overlap analysis
        if len(txn1_screens) > 0 and len(txn2_screens) > 0 and txn1_screens[0] != 'No flow data' and txn2_screens[0] != 'No flow data':
            txn1_set = set(txn1_screens)
            txn2_set = set(txn2_screens)
            
            common_screens = txn1_set & txn2_set
            unique_txn1 = txn1_set - txn2_set
            unique_txn2 = txn2_set - txn1_set
            logger.info("Screen comparison - common: %d, txn1 unique: %d, txn2 unique: %d",
                    len(common_screens), len(unique_txn1), len(unique_txn2))
            
            analysis.append(f"**Screen Usage Comparison:**")
            analysis.append(f"   - Common Screens: {len(common_screens)}")
            analysis.append(f"   - Transaction 1 Only: {len(unique_txn1)}")
            analysis.append(f"   - Transaction 2 Only: {len(unique_txn2)}")
            
            if unique_txn1:
                analysis.append(f"   - Transaction 1 Unique: {', '.join(sorted(unique_txn1))}")
            if unique_txn2:
                analysis.append(f"   - Transaction 2 Unique: {', '.join(sorted(unique_txn2))}")
            
            total_unique_screens = len(txn1_set | txn2_set)
            analysis.append(f"   - Total Unique Screens Used: {total_unique_screens}")
        else:
            logger.warning("Insufficient flow data to analyze screens")
            analysis.append(f"**Screen Usage:** Cannot analyze - insufficient flow data")
        
        analysis.append("")
        
        # Source file information (if available)
        analysis.append(f"**Data Source:**")
        if 'Source_File' in txn1_data.index:
            analysis.append(f"   - Transaction 1 Source: {txn1_data['Source_File']}")
        if 'Source_File' in txn2_data.index:
            analysis.append(f"   - Transaction 2 Source: {txn2_data['Source_File']}")
        
        same_source = False
        if 'Source_File' in txn1_data.index and 'Source_File' in txn2_data.index:
            same_source = txn1_data['Source_File'] == txn2_data['Source_File']
            logger.info("Transactions come from the same source file: %s", same_source)
            analysis.append(f"   - Same Source File: {'Yes' if same_source else 'No'}")
        
        analysis.append("")
        
        logger.info("Comparison analysis completed for %s vs %s", txn1_id, txn2_id)
        
        return "\n".join(analysis)