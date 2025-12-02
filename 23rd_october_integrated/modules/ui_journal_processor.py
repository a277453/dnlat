"""
UI Journal Processor Module

This module contains classes and functions for processing ATM UI journal files (.jrn),
mapping transactions to UI flows, and generating comprehensive flow analysis reports.

Classes:
    UIJournalProcessor: Main class for processing UI journal files

Functions:
    parse_ui_journal(): Parse .jrn UI journal files into structured DataFrames
    map_transactions_and_generate_report(): Map transactions to UI flows and generate reports
    process_multiple_ui_journals(): Process multiple UI journal files in batch
"""

import re
import json
import pandas as pd
from pathlib import Path
from typing import Union, List, Dict, Tuple
from datetime import datetime


class UIJournalProcessor:
    """
    A class for processing ATM UI journal files and analyzing transaction flows.
    
    Attributes:
        file_path (Path): Path to the UI journal file
        df (pd.DataFrame): Parsed DataFrame of UI events
        
    Methods:
        load_journal(): Load and parse the UI journal file
        get_events_in_timerange(): Get UI events within a specific time range
        get_screen_flow(): Extract screen flow for a transaction
        export_to_csv(): Export parsed data to CSV
    """
    
    def __init__(self, file_path: Union[str, Path]):
        """
        Initialize the UIJournalProcessor.
        
        Args:
            file_path: Path to the .jrn UI journal file
        """
        self.file_path = Path(file_path)
        self.df = None
        
    def load_journal(self) -> pd.DataFrame:
        """
        Load and parse the UI journal file.
        
        Returns:
            pd.DataFrame: Parsed UI events data
        """
        self.df = parse_ui_journal(self.file_path)
        return self.df
    
    def get_events_in_timerange(self, start_time, end_time) -> pd.DataFrame:
        """
        Get UI events within a specific time range.
        
        Args:
            start_time: Start time (datetime.time object)
            end_time: End time (datetime.time object)
            
        Returns:
            pd.DataFrame: Filtered UI events
        """
        if self.df is None:
            raise ValueError("Journal not loaded. Call load_journal() first.")
        
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'], errors='coerce')
        
        filtered = self.df[
            (self.df['timestamp'].dt.time >= start_time) & 
            (self.df['timestamp'].dt.time <= end_time)
        ].copy()
        
        return filtered
    
    def get_screen_flow(self, start_time, end_time) -> List[str]:
        """
        Extract the screen flow for a transaction within a time range.
        
        Args:
            start_time: Transaction start time
            end_time: Transaction end time
            
        Returns:
            List[str]: List of screen names in order
        """
        events = self.get_events_in_timerange(start_time, end_time)
        
        if len(events) == 0:
            return []
        
        events = events.sort_values('timestamp')
        flow = []
        prev_screen = None
        
        for _, row in events.iterrows():
            current_screen = row['screen']
            if current_screen != prev_screen:
                flow.append(current_screen)
                prev_screen = current_screen
        
        return flow
    
    def export_to_csv(self, output_path: Union[str, Path]) -> str:
        """
        Export parsed UI events to CSV.
        
        Args:
            output_path: Path for the output CSV file
            
        Returns:
            str: Path to the created CSV file
        """
        if self.df is None:
            raise ValueError("Journal not loaded. Call load_journal() first.")
        
        output_path = Path(output_path)
        self.df.to_csv(output_path, index=False)
        return str(output_path)


def parse_ui_journal(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Parse a .jrn UI journal file and return a structured DataFrame.
    
    This function processes ATM UI journal files, extracting structured data including
    timestamps, screen names, events, and JSON data from each log entry.
    
    Args:
        file_path: Path to the .jrn UI journal file
        
    Returns:
        pd.DataFrame: Parsed UI events with columns:
            - date: Date of the event
            - timestamp: Time of the event
            - id: Log entry ID
            - module: Module name (e.g., GUIDM)
            - direction: Direction indicator (<, >, *)
            - viewid: View ID number
            - screen: Screen/state name
            - event_type: Type of event (result or action)
            - raw_json: Raw JSON data from the event
            - json_*: Parsed JSON fields as separate columns
            
    Example:
        >>> df = parse_ui_journal('ui_journal.jrn')
        >>> print(df[['timestamp', 'screen', 'event_type']].head())
    """
    file_path = Path(file_path)
    if not file_path.exists() or file_path.is_dir():
        print(f"Error: File {file_path} does not exist or is a directory.")
        return pd.DataFrame()

    # Regex patterns for parsing log entries
    pattern_no_date = re.compile(
        r'^(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+\[(\d+)\]\s+-\s+(\w+)\s+(result|action):(.+)$'
    )
    pattern_with_date = re.compile(
        r'^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+\[(\d+)\]\s+-\s+(\w+)\s+(result|action):(.+)$'
    )

    # Extract date from filename
    filename = file_path.stem
    date_match = re.search(r'(\d{8})', filename)
    if date_match:
        date_str = date_match.group(1)
        try:
            file_date = datetime.strptime(date_str, '%Y%m%d').strftime('%d/%m/%Y')
        except ValueError:
            file_date = filename
    else:
        file_date = filename

    # First pass: clean and deduplicate lines
    cleaned_lines = []
    processed_lines = set()

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            match = pattern_no_date.match(line)
            if not match:
                continue

            timestamp, log_id, module, direction, view_id, screen, event_type, event_data = match.groups()

            # Filter for result and action events only
            if event_type not in ["result", "action"]:
                continue

            # Filter GUIDM module for DMAuthorization screen only
            if module == "GUIDM" and screen != "DMAuthorization":
                continue

            # Validate JSON data
            try:
                json.loads(event_data)
            except json.JSONDecodeError:
                continue

            # Add date to line and deduplicate
            cleaned_line = f"{file_date} {timestamp}  {log_id} {module} {direction} [{view_id}] - {screen} {event_type}:{event_data}"
            if cleaned_line not in processed_lines:
                processed_lines.add(cleaned_line)
                cleaned_lines.append(cleaned_line)

    if not cleaned_lines:
        print(f"Warning: No valid lines found in {file_path}")
        return pd.DataFrame()
   
    # Second pass: parse cleaned lines into structured data
    parsed_data = []
    for line in cleaned_lines:
        line = line.strip()
        if not line:
            continue

        m = pattern_with_date.match(line)
        has_date = True
        if not m:
            m = pattern_no_date.match(line)
            has_date = False

        if not m:
            continue

        # Extract fields based on pattern match
        if has_date:
            date, timestamp, log_id, module, direction, view_id, screen, event_type, event_data = m.groups()
        else:
            timestamp, log_id, module, direction, view_id, screen, event_type, event_data = m.groups()
            date = None

        # Parse JSON data
        try:
            json_data = json.loads(event_data)
        except json.JSONDecodeError:
            json_data = {}

        # Build row dictionary
        row = {
            "date": date,
            "timestamp": timestamp,
            "id": int(log_id),
            "module": module,
            "direction": direction,
            "viewid": int(view_id),
            "screen": screen,
            "event_type": event_type,
            "raw_json": event_data
        }

        # Add formatted date fields
        if date:
            try:
                date_obj = datetime.strptime(date, "%d/%m/%Y")
                row["date_formatted"] = date_obj.strftime("%Y-%m-%d")
                row["day_of_week"] = date_obj.strftime("%A")
            except ValueError:
                row["date_formatted"] = date
                row["day_of_week"] = None
        else:
            row["date_formatted"] = None
            row["day_of_week"] = None

        # Flatten JSON fields into separate columns
        for k, v in json_data.items():
            if isinstance(v, (int, float)):
                row[f"json_{k}"] = v
            elif isinstance(v, str):
                try:
                    if "." not in v:
                        row[f"json_{k}"] = int(v)
                    else:
                        row[f"json_{k}"] = float(v)
                except ValueError:
                    row[f"json_{k}"] = v
            else:
                row[f"json_{k}"] = str(v)

        parsed_data.append(row)
   
    if not parsed_data:
        print("Warning: No parsed data.")
        return pd.DataFrame()
    
    df = pd.DataFrame(parsed_data)
    return df


def map_transactions_and_generate_report(
    transaction_df: pd.DataFrame, 
    ui_df: pd.DataFrame, 
    output_file: str = 'transaction_flows.txt'
) -> str:
    """
    Map transactions to UI flows and generate a comprehensive flow analysis report.
    
    This function correlates transaction data with UI events based on timestamps,
    extracts screen flows, and generates a detailed text report showing how
    each transaction progressed through different UI screens.
    
    Args:
        transaction_df: DataFrame with transaction data containing:
            - Transaction ID: Unique transaction identifier
            - Transaction Type: Type of transaction (e.g., Withdrawal, Balance)
            - Start Time: Transaction start time
            - End Time: Transaction end time
        ui_df: DataFrame with UI events (from parse_ui_journal)
        output_file: Path for the output report file (default: 'transaction_flows.txt')
        
    Returns:
        str: Path to the generated report file
        
    Raises:
        ValueError: If required columns are missing from input DataFrames
        
    Example:
        >>> transaction_df = pd.read_csv('transactions.csv')
        >>> ui_df = parse_ui_journal('ui_journal.jrn')
        >>> report_path = map_transactions_and_generate_report(transaction_df, ui_df)
        >>> print(f"Report generated: {report_path}")
    """
    # Validate required columns
    required_transaction_cols = ['Transaction ID', 'Transaction Type', 'Start Time', 'End Time']
    required_ui_cols = ['timestamp', 'screen', 'date', 'event_type', 'json_resultDetail', 'json_action']

    missing_transaction_cols = [col for col in required_transaction_cols if col not in transaction_df.columns]
    missing_ui_cols = [col for col in required_ui_cols if col not in ui_df.columns]

    if missing_transaction_cols:
        raise ValueError(f"Missing columns in transaction file: {missing_transaction_cols}")
    if missing_ui_cols:
        raise ValueError(f"Missing columns in UI file: {missing_ui_cols}")

    # Ensure timestamp is datetime format
    ui_df['timestamp'] = pd.to_datetime(ui_df['timestamp'], errors='coerce')
    results = []

    # Process each transaction
    for idx, transaction in transaction_df.iterrows():
        transaction_id = transaction['Transaction ID']
        transaction_type = transaction['Transaction Type']
        start_time = transaction['Start Time']
        end_time = transaction['End Time']

        # Skip transactions with missing times
        if pd.isna(start_time) or pd.isna(end_time):
            continue

        # Filter UI events for this transaction's time range
        ui_events = ui_df[
            (ui_df['timestamp'].dt.time >= start_time) & 
            (ui_df['timestamp'].dt.time <= end_time)
        ].copy()

        # Handle case with no UI events
        if len(ui_events) == 0:
            results.append({
                'row_number': idx + 1,
                'transaction_id': transaction_id,
                'transaction_type': transaction_type,
                'start_time': start_time,
                'end_time': end_time,
                'screen_flow': [],
                'ui_events_count': 0,
                'ui_dates': []
            })
            continue

        # Build screen flow
        ui_events = ui_events.sort_values('timestamp')
        flow_parts = []
        prev_screen = None

        for _, row in ui_events.iterrows():
            current_screen = row['screen']
            timestamp_str = row['timestamp'].strftime('%H:%M:%S') if pd.notna(row['timestamp']) else ''
            screen_with_time = f"{current_screen}[{timestamp_str}]"

            # Determine transition detail based on event type
            event_type = str(row['event_type']).lower() if pd.notna(row['event_type']) else ''
            if event_type == 'result':
                detail = row['json_resultDetail'] if pd.notna(row['json_resultDetail']) else 'RESULT'
            elif event_type == 'action':
                detail = row['json_action'] if pd.notna(row['json_action']) else 'ACTION'
            else:
                detail = 'OK'

            # Add to flow if screen changed
            if screen_with_time != prev_screen:
                flow_parts.append(screen_with_time)
                flow_parts.append(f"--{detail}-->")
                prev_screen = screen_with_time

        # Extract unique dates from UI events
        ui_events['date'] = pd.to_datetime(ui_events['date'], errors='coerce')
        ui_dates = ui_events['date'].dropna().dt.date.unique().tolist()

        results.append({
            'row_number': idx + 1,
            'transaction_id': transaction_id,
            'transaction_type': transaction_type,
            'start_time': start_time,
            'end_time': end_time,
            'screen_flow': flow_parts,
            'ui_events_count': len(ui_events),
            'ui_dates': ui_dates
        })

    # Write report to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("TRANSACTION UI FLOW ANALYSIS\n")
        f.write("=" * 80 + "\n\n")
    
        for result in results:
            f.write(f"Transaction ID: {result['transaction_id']}\n")
            f.write(f"Transaction Type: {result['transaction_type']}\n")
            f.write(f"Start Time: {result['start_time'].strftime('%H:%M:%S')}\n")
            f.write(f"End Time: {result['end_time'].strftime('%H:%M:%S')}\n")
            f.write(f"UI Events: {result['ui_events_count']}\n")
        
            if result['ui_dates']:
                f.write(f"UI Dates: {', '.join(str(d) for d in result['ui_dates'])}\n")
            else:
                f.write("UI Dates: No date data available\n")

            flow_text = " ".join(result['screen_flow']) if result['screen_flow'] else "No screen data available"
            f.write(f"Flow: {flow_text}\n")
            f.write("\n" + "-" * 60 + "\n\n")

    return output_file


def process_multiple_ui_journals(
    journal_files: List[Union[str, Path]], 
    output_dir: Union[str, Path] = None
) -> Dict[str, pd.DataFrame]:
    """
    Process multiple UI journal files in batch.
    
    This function processes multiple .jrn files and returns a dictionary
    of DataFrames, optionally saving each to CSV in the output directory.
    
    Args:
        journal_files: List of paths to .jrn UI journal files
        output_dir: Optional directory to save CSV outputs (default: None)
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping file names to parsed DataFrames
        
    Example:
        >>> files = ['ui_log_20250101.jrn', 'ui_log_20250102.jrn']
        >>> results = process_multiple_ui_journals(files, output_dir='./outputs')
        >>> print(f"Processed {len(results)} files")
    """
    results = {}
    
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    for file_path in journal_files:
        file_path = Path(file_path)
        
        if not file_path.exists():
            print(f"Warning: File not found: {file_path}")
            continue
        
        print(f"Processing: {file_path.name}")
        
        # Parse the journal file
        df = parse_ui_journal(file_path)
        
        if df.empty:
            print(f"Warning: No data parsed from {file_path.name}")
            continue
        
        # Store result
        file_key = file_path.stem
        results[file_key] = df
        
        # Optionally save to CSV
        if output_dir:
            csv_path = output_dir / f"{file_key}_parsed.csv"
            df.to_csv(csv_path, index=False)
            print(f"  Saved to: {csv_path}")
    
    print(f"\nCompleted: Processed {len(results)} of {len(journal_files)} files")
    return results


# Example usage
if __name__ == "__main__":
    # Example 1: Using the UIJournalProcessor class
    print("Example 1: Using UIJournalProcessor class")
    processor = UIJournalProcessor("sample_ui_journal.jrn")
    df = processor.load_journal()
    print(f"Loaded {len(df)} UI events")
    
    # Example 2: Using standalone functions
    print("\nExample 2: Using standalone functions")
    ui_df = parse_ui_journal("sample_ui_journal.jrn")
    # Assuming you have a transaction DataFrame
    # report = map_transactions_and_generate_report(transaction_df, ui_df)
    
    # Example 3: Batch processing
    print("\nExample 3: Batch processing multiple files")
    files = ["file1.jrn", "file2.jrn", "file3.jrn"]
    results = process_multiple_ui_journals(files, output_dir="./parsed_ui_data")