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
from modules.logging_config import logger
import logging


logger.info("UI Journal Processor loaded")


class UIJournalProcessor:
    """
    CLASS: UIJournalProcessor

    DESCRIPTION:
        Handles processing of ATM UI journal (.jrn) files for extracting UI events,
        filtering based on time, generating screen flows, and exporting results.

    USAGE:
        processor = UIJournalProcessor("file.jrn")
        df = processor.load_journal()

    PARAMETERS:
        file_path (str | Path) : Path of the UI journal file to load.

    RETURNS:
        None

    RAISES:
        FileNotFoundError : When journal file does not exist.
        ValueError        : When operations are called before loading data.
    """
    
    def __init__(self, file_path: Union[str, Path]):
        """Initialize processor with file path."""
        self.file_path = Path(file_path)
        self.df = None
        logger.debug(f"UIJournalProcessor initialized with file: {self.file_path}")
        
    def load_journal(self) -> pd.DataFrame:
        """
        FUNCTION: load_journal

        DESCRIPTION:
            Loads and parses the UI journal file into a DataFrame.

        USAGE:
            df = processor.load_journal()

        PARAMETERS:
            None

        RETURNS:
            DataFrame : Parsed UI events.

        RAISES:
            FileNotFoundError : If file is missing.
        """
        logger.info(f"Loading journal: {self.file_path}")
        self.df = parse_ui_journal(self.file_path)
        logger.info(f"Loaded {len(self.df)} UI events")
        return self.df
    
    def get_events_in_timerange(self, start_time, end_time) -> pd.DataFrame:
        """
        FUNCTION: get_events_in_timerange

        DESCRIPTION:
            Filters UI events between the given start and end times.

        USAGE:
            events = processor.get_events_in_timerange(start, end)

        PARAMETERS:
            start_time (datetime.time) : Start time boundary.
            end_time   (datetime.time) : End time boundary.

        RETURNS:
            DataFrame : Filtered rows within the time range.

        RAISES:
            ValueError : If journal is not loaded.
        """
        logger.debug(f"Filtering events from {start_time} to {end_time}")
        if self.df is None:
            logger.error("Journal not loaded. Call load_journal() first.")
            raise ValueError("Journal not loaded. Call load_journal() first.")
        
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'], errors='coerce')
        
        filtered = self.df[
            (self.df['timestamp'].dt.time >= start_time) & 
            (self.df['timestamp'].dt.time <= end_time)
        ].copy()
        
        logger.info(f"Filtered {len(filtered)} events in the specified time range")
        return filtered
    
    def get_screen_flow(self, start_time, end_time) -> List[str]:
        """
        FUNCTION: get_screen_flow

        DESCRIPTION:
            Generates an ordered list of unique screens visited in the given time range.

        USAGE:
            flow = processor.get_screen_flow(start, end)

        PARAMETERS:
            start_time (datetime.time) : Start time.
            end_time   (datetime.time) : End time.

        RETURNS:
            list : Ordered list of screen names.

        RAISES:
            ValueError : If journal not loaded or no events found.
        """
        logger.debug(f"Getting screen flow from {start_time} to {end_time}")
        events = self.get_events_in_timerange(start_time, end_time)
        
        if len(events) == 0:
            logger.warning("No events found in the specified time range")
            return []
        
        events = events.sort_values('timestamp')
        flow = []
        prev_screen = None
        
        for _, row in events.iterrows():
            current_screen = row['screen']
            if current_screen != prev_screen:
                flow.append(current_screen)
                prev_screen = current_screen
        
        logger.info(f"Screen flow extracted: {flow}")
        return flow
    
    def export_to_csv(self, output_path: Union[str, Path]) -> str:
        """
        FUNCTION: export_to_csv

        DESCRIPTION:
            Exports the parsed UI journal DataFrame to a CSV file.

        USAGE:
            processor.export_to_csv("output.csv")

        PARAMETERS:
            output_path (str | Path) : Target output CSV path.

        RETURNS:
            str : Path of saved CSV.

        RAISES:
            ValueError : If journal not loaded.
        """
        if self.df is None:
            logger.error("Journal not loaded. Cannot export to CSV")
            raise ValueError("Journal not loaded. Call load_journal() first.")
        
        output_path = Path(output_path)
        self.df.to_csv(output_path, index=False)
        logger.info(f"Exported parsed data to CSV: {output_path}")
        return str(output_path)


def parse_ui_journal(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    FUNCTION: parse_ui_journal

    DESCRIPTION:
        Parses a .jrn UI journal file and extracts structured event data.

    USAGE:
        df = parse_ui_journal("file.jrn")

    PARAMETERS:
        file_path (str | Path) : Path of UI journal file.

    RETURNS:
        DataFrame : Parsed UI journal events with timestamps, screens, and JSON fields.

    RAISES:
        None
    """
    logger.info(f"Parsing UI journal: {file_path}")
    file_path = Path(file_path)
    if not file_path.exists() or file_path.is_dir():
        logger.error(f"File {file_path} does not exist or is a directory.")
        return pd.DataFrame()

    # Regex patterns...
    pattern_no_date = re.compile(
        r'^(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+\[(\d+)\]\s+-\s+(\w+)\s+(result|action):(.+)$'
    )
    pattern_with_date = re.compile(
        r'^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+\[(\d+)\]\s+-\s+(\w+)\s+(result|action):(.+)$'
    )

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

            if event_type not in ["result", "action"]:
                continue

            if module == "GUIDM" and screen != "DMAuthorization":
                continue

            try:
                json.loads(event_data)
            except json.JSONDecodeError:
                continue

            cleaned_line = f"{file_date} {timestamp}  {log_id} {module} {direction} [{view_id}] - {screen} {event_type}:{event_data}"
            if cleaned_line not in processed_lines:
                processed_lines.add(cleaned_line)
                cleaned_lines.append(cleaned_line)

    if not cleaned_lines:
        logger.warning(f"No valid lines found in {file_path}")
        return pd.DataFrame()

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

        if has_date:
            date, timestamp, log_id, module, direction, view_id, screen, event_type, event_data = m.groups()
        else:
            timestamp, log_id, module, direction, view_id, screen, event_type, event_data = m.groups()
            date = None

        try:
            json_data = json.loads(event_data)
        except json.JSONDecodeError:
            json_data = {}

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
        logger.warning("No parsed data.")
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)
    logger.info(f"Parsed {len(df)} events from {file_path}")
    return df


def process_multiple_ui_journals(
    journal_files: List[Union[str, Path]], 
    output_dir: Union[str, Path] = None
) -> Dict[str, pd.DataFrame]:
    """
    FUNCTION: process_multiple_ui_journals

    DESCRIPTION:
        Processes multiple UI journal files and optionally exports results to CSV.

    USAGE:
        results = process_multiple_ui_journals(files, "./output")

    PARAMETERS:
        journal_files (list)    : List of file paths to process.
        output_dir (str | Path) : Directory to save CSV output (optional).

    RETURNS:
        dict : Mapping of filename → parsed DataFrame.

    RAISES:
        None
    """
    results = {}
    
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    for file_path in journal_files:
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            continue
        
        logger.info(f"Processing file: {file_path.name}")
        df = parse_ui_journal(file_path)
        
        if df.empty:
            logger.warning(f"No data parsed from {file_path.name}")
            continue
        
        file_key = file_path.stem
        results[file_key] = df
        
        if output_dir:
            csv_path = output_dir / f"{file_key}_parsed.csv"
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved CSV: {csv_path}")
    
    logger.info(f"Completed: Processed {len(results)} of {len(journal_files)} files")
    return results


if __name__ == "__main__":
    # Example 1: Using the UIJournalProcessor class
    logger.info("Example 1: Using UIJournalProcessor class")
    processor = UIJournalProcessor("sample_ui_journal.jrn")
    df = processor.load_journal()
    logger.info(f"Loaded {len(df)} UI events")
    
    # Example 2: Using standalone functions
    logger.info("\nExample 2: Using standalone functions")
    ui_df = parse_ui_journal("sample_ui_journal.jrn")
    # Assuming you have a transaction DataFrame
    # report = map_transactions_and_generate_report(transaction_df, ui_df)
    
    # Example 3: Batch processing
    logger.info("\nExample 3: Batch processing multiple files")
    files = ["file1.jrn", "file2.jrn", "file3.jrn"]
    results = process_multiple_ui_journals(files, output_dir="./parsed_ui_data")

