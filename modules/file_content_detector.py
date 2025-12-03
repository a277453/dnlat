import os
import re
from pathlib import Path
from typing import Optional, List

def try_read_file(filepath: str) -> Optional[str]:
    """Try to read file with different encodings"""
    encodings = ['utf-8', 'latin1', 'windows-1252', 'utf-16']
    
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
                content = f.read()
                return content
        except Exception as e:
            continue
    
    # If all fail, try binary mode and decode with errors ignored
    try:
        with open(filepath, 'rb') as f:
            content = f.read().decode('utf-8', errors='ignore')
            return content
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def detect_ui_journal_pattern(lines: list) -> int:
    """
    Detect UI Journal pattern matches
    Pattern: timestamp id module direction [viewid] - screen event:{json}
    """
    ui_matches = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        ui_indicators = 0
        if re.search(r'\s+[<>*]\s+', line): ui_indicators += 1
        if re.search(r'\[\d+\]', line): ui_indicators += 1
        if ' - ' in line: ui_indicators += 1
        if re.search(r'(result|action):\s*\{.*\}', line): ui_indicators += 1
        if re.search(r'^\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line) or \
           re.search(r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line):
            ui_indicators += 1
        
        if ui_indicators >= 4:
            ui_matches += 1
    
    return ui_matches

def detect_customer_journal_pattern(lines: list) -> int:
    """
    Detect Customer Journal pattern matches
    Pattern: timestamp tid message
    """
    customer_matches = 0
    
    for line in lines:
        line = line.strip()
        if not line or set(line) <= {'*'}:
            continue
        
        basic_match = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(\d+)\s*(.*)", line)
        if not basic_match:
            continue
        
        non_ui_indicators = 0
        if not re.search(r'\s+[<>*]\s+', line): non_ui_indicators += 1
        if not re.search(r'\[\d+\]', line): non_ui_indicators += 1
        if ' - ' not in line: non_ui_indicators += 1
        if not re.search(r'(result|action):\s*\{.*\}', line): non_ui_indicators += 1
        
        tid = basic_match.group(2)
        if tid in ['3201', '3202', '3207', '3217', '3220']:
            non_ui_indicators += 1
        
        if non_ui_indicators >= 4:
            customer_matches += 1
    
    return customer_matches

def detect_trc_trace_pattern(lines: list) -> int:
    """
    Detect TRC Trace pattern matches
    Pattern: event_num date timestamp module device PID:xxx.xxx Data:xxx
    """
    matches = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        if re.search(r'\d{2}:\d{2}:\d{2}\.\d{2}', line):
            if re.search(r'PID:\w+\.\w+', line):
                if 'Data:' in line:
                    matches += 1
    
    return matches

def detect_trc_error_pattern(lines: list) -> int:
    """
    Detect TRC Error pattern matches
    Pattern: AA/BB YYMMDD HH:MM:SS.MS ErrorName ModuleName PID:xxx.xxx Data:xxx
    """
    trc_error_matches = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        trc_error_header_pattern = r'^\d{2}/\d{2}\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{1,3}\s+\w+\s+\w+\s+PID:\w+\.\w+\s+Data:\d+'
        
        if re.match(trc_error_header_pattern, line):
            trc_error_matches += 1
            continue
        
        if line.startswith('*** Running'):
            trc_error_matches += 1
            continue
        
        if line.startswith('Created by'):
            trc_error_matches += 1
            continue
        
        if line == 'Process Information:':
            trc_error_matches += 1
            continue
    
    return trc_error_matches

def count_trc_error_headers(lines: list) -> int:
    """Count only the TRC Error header patterns (AA/BB YYMMDD format)"""
    header_matches = 0
    trc_error_header_pattern = r'^\d{2}/\d{2}\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{1,3}\s+\w+\s+\w+\s+PID:\w+\.\w+\s+Data:\d+'
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        if re.match(trc_error_header_pattern, line):
            header_matches += 1
    
    return header_matches

def detect_file_type_by_content(file_path: str) -> str:
    """
    Main function to detect file type based on pattern matching and file extension validation.
    This is the more robust logic from the reference file.
    """
    if not Path(file_path).exists():
        return "Error: File not found"
    
    file_ext = Path(file_path).suffix.lower()
    
    # Skip common non-log file types immediately to improve performance
    if file_ext in ['.py', '.js', '.html', '.css', '.json', '.xml', '.txt', '.xlsx', '.xls', '.csv', '.pdf', '.doc', '.docx']:
        return "Unidentified"
        
    content = try_read_file(file_path)
    if content is None:
        return "Error: Could not read file"
    
    lines = content.split('\n')
    non_empty_lines = [line for line in lines if line.strip()]
    
    if len(non_empty_lines) < 5:
        return "Insufficient data"
    
    # Count pattern matches for each file type
    ui_matches = detect_ui_journal_pattern(lines)
    customer_matches = detect_customer_journal_pattern(lines)
    trc_matches = detect_trc_trace_pattern(lines)
    trc_error_matches = detect_trc_error_pattern(lines)
    
    max_matches = max(ui_matches, customer_matches, trc_matches, trc_error_matches)
    
    if max_matches < 5:
        return "Unidentified"

    # --- New, More Accurate Decision Logic ---

    # For .prn files, TRC Error is more specific than TRC Trace. Prioritize it.
    if file_ext == '.prn':
        trc_error_header_matches = count_trc_error_headers(lines)
        if trc_error_header_matches >= 5: return "TRC Error"
        if trc_error_matches == max_matches: return "TRC Error"
        if trc_matches == max_matches: return "TRC Trace"
        return "Unidentified"
    
    # For .jrn files, differentiate between UI and Customer journals.
    elif file_ext == '.jrn':
        if ui_matches > customer_matches and ui_matches > 5: return "UI Journal"
        if customer_matches > ui_matches and customer_matches > 5: return "Customer Journal"
        return "Unidentified"
    
    # For other files (like .001, .002), rely on the highest score.
    else:
        if trc_error_matches == max_matches: return "TRC Error"
        if ui_matches == max_matches: return "UI Journal"
        if customer_matches == max_matches: return "Customer Journal"
        if trc_matches == max_matches: return "TRC Trace"
        return "Unidentified"