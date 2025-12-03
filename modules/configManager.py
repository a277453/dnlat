import xmltodict  # type: ignore
import re
from pathlib import Path
from typing import Optional

def xml_to_dict(xml_file):
    """
    Parses an XML configuration file to extract transaction metadata and parsing boundaries.
    
    Args:
        xml_file (str): Path to the XML file containing configuration settings.
    
    Returns:
        tuple:
            - real_name (dict): A dictionary mapping transaction codes to their preferred names.
              For example: {"COUT": "Withdrawal", "CIN": "Deposit"}
            - start_time_list (list): A list of keywords used to identify transaction start lines.
            - end_time_list (list): A list of keywords used to identify transaction end lines.
            - chain_time_list (list): A list of keywords used to identify transaction chaining lines.
    
    Notes:
        - Expects the XML to have this structure:
          <configuration>
            <transactionList>
              <transaction>
                <key>...</key>
                <value>...</value>
              </transaction>
              ...
            </transactionList>
            <customerJournalParsing>
              <starttransaction>start1,start2,...</starttransaction>
              <endtransaction>end1,end2,...</endtransaction>
              <chainingtransaction>chain1,chain2,...</chainingtransaction>
            </customerJournalParsing>
          </configuration>
    """
    with open(xml_file, 'r', encoding='utf-8') as file:
        txn_xml = file.read()
    
    config_dict = xmltodict.parse(txn_xml)
    
    # Extract transaction mappings
    real_name = {
        txn['key']: txn['value']
        for txn in config_dict['configuration']['transactionList']['transaction']
    }
    
    # Extract start transaction TIDs
    start_time_list = config_dict['configuration']['customerJournalParsing']['starttransaction'].split(',')
    start_time_list = [tid.strip() for tid in start_time_list]  # Remove whitespace
    
    # Extract end transaction TIDs
    end_time_list = config_dict['configuration']['customerJournalParsing']['endtransaction'].split(',')
    end_time_list = [tid.strip() for tid in end_time_list]  # Remove whitespace
    
    # Extract chaining transaction TIDs (with fallback if not present)
    chain_time_list = []
    try:
        chaining_element = config_dict['configuration']['customerJournalParsing'].get('chainingtransaction', '')
        if chaining_element:
            chain_time_list = chaining_element.split(',')
            chain_time_list = [tid.strip() for tid in chain_time_list if tid.strip()]  # Remove whitespace and empty strings
    except (KeyError, AttributeError):
        # If chainingtransaction is not present in XML, return empty list
        chain_time_list = []
    
    return real_name, start_time_list, end_time_list, chain_time_list


# Optional: Helper function to validate configuration
def validate_xml_config(xml_file):
    """
    Validates that the XML configuration file has all required sections.
    
    Args:
        xml_file (str): Path to the XML file
        
    Returns:
        dict: Validation results with status and any missing sections
    """
    try:
        with open(xml_file, 'r', encoding='utf-8') as file:
            txn_xml = file.read()
        
        config_dict = xmltodict.parse(txn_xml)
        
        validation_result = {
            'valid': True,
            'missing_sections': [],
            'warnings': []
        }
        
        # Check required sections
        required_sections = [
            'configuration',
            'configuration.transactionList',
            'configuration.customerJournalParsing',
            'configuration.customerJournalParsing.starttransaction',
            'configuration.customerJournalParsing.endtransaction'
        ]
        
        for section in required_sections:
            keys = section.split('.')
            current = config_dict
            try:
                for key in keys:
                    current = current[key]
            except (KeyError, TypeError):
                validation_result['valid'] = False
                validation_result['missing_sections'].append(section)
        
        # Check optional sections and warn if missing
        optional_sections = [
            'configuration.customerJournalParsing.chainingtransaction'
        ]
        
        for section in optional_sections:
            keys = section.split('.')
            current = config_dict
            try:
                for key in keys:
                    current = current[key]
            except (KeyError, TypeError):
                validation_result['warnings'].append(f"Optional section missing: {section}")
        
        return validation_result
        
    except Exception as e:
        return {
            'valid': False,
            'error': f"Failed to parse XML: {str(e)}",
            'missing_sections': [],
            'warnings': []
        }


# Optional: Helper function to get all TID lists at once
def get_all_tids(xml_file):
    """
    Convenience function to get all TID lists with descriptive names.
    
    Args:
        xml_file (str): Path to the XML file
        
    Returns:
        dict: Dictionary containing all TID lists
    """
    real_name, start_tids, end_tids, chain_tids = xml_to_dict(xml_file)
    
    return {
        'transaction_names': real_name,
        'start_tids': start_tids,
        'end_tids': end_tids,
        'chain_tids': chain_tids,
        'all_parsing_tids': start_tids + end_tids + chain_tids
    }


# Optional: Debug function to print configuration
def debug_print_config(xml_file):
    """
    Debug function to print the parsed configuration.
    
    Args:
        xml_file (str): Path to the XML file
    """
    try:
        real_name, start_tids, end_tids, chain_tids = xml_to_dict(xml_file)
        
        print("=" * 60)
        print("XML Configuration Debug Info")
        print("=" * 60)
        
        print(f"\nTransaction Types ({len(real_name)}):")
        for key, value in real_name.items():
            print(f"  {key} → {value}")
        
        print(f"\nStart Transaction TIDs ({len(start_tids)}):")
        print(f"  {', '.join(start_tids)}")
        
        print(f"\nEnd Transaction TIDs ({len(end_tids)}):")
        print(f"  {', '.join(end_tids)}")
        
        print(f"\nChain Transaction TIDs ({len(chain_tids)}):")
        if chain_tids:
            print(f"  {', '.join(chain_tids)}")
        else:
            print("  None configured")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"Error debugging configuration: {e}")


# Optional: Function to update XML configuration programmatically
def add_chain_tid_to_xml(xml_file, new_chain_tid, backup=True):
    """
    Adds a new chaining TID to the XML configuration.
    
    Args:
        xml_file (str): Path to the XML file
        new_chain_tid (str): New TID to add to chaining list
        backup (bool): Whether to create a backup before modifying
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if backup:
            import shutil
            shutil.copy2(xml_file, f"{xml_file}.backup")
        
        with open(xml_file, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Simple string replacement approach
        if '<chainingtransaction>' in content:
            # Find existing chaining section and append
            import re
            pattern = r'<chainingtransaction>([^<]*)</chainingtransaction>'
            match = re.search(pattern, content)
            if match:
                current_tids = match.group(1).strip()
                if current_tids:
                    new_tids = f"{current_tids},{new_chain_tid}"
                else:
                    new_tids = new_chain_tid
                
                new_content = re.sub(pattern, f'<chainingtransaction>{new_tids}</chainingtransaction>', content)
            else:
                return False
        else:
            # Add new chaining section
            insert_point = content.find('</customerJournalParsing>')
            if insert_point == -1:
                return False
            
            new_section = f'<chainingtransaction>{new_chain_tid}</chainingtransaction>\n'
            new_content = content[:insert_point] + new_section + content[insert_point:]
        
        with open(xml_file, 'w', encoding='utf-8') as file:
            file.write(new_content)
        
        return True
        
    except Exception as e:
        print(f"Error updating XML: {e}")
        return False


# FILE TYPE DETECTION FUNCTIONS
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
    Key distinguishing features:
    - Has direction symbols: < > *
    - Has [viewid] in square brackets
    - Has " - " separator
    - Has "result:" or "action:" followed by JSON
    """
    ui_matches = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Check for UI-specific markers in order of specificity
        ui_indicators = 0
        
        # 1. Must have direction symbols (< > *)
        if re.search(r'\s+[<>*]\s+', line):
            ui_indicators += 1
        
        # 2. Must have [number] pattern (viewid)
        if re.search(r'\[\d+\]', line):
            ui_indicators += 1
        
        # 3. Must have " - " separator
        if ' - ' in line:
            ui_indicators += 1
        
        # 4. Must have result: or action: followed by what looks like JSON
        if re.search(r'(result|action):\s*\{.*\}', line):
            ui_indicators += 1
        
        # 5. Should have module name (like GUIAPP, etc.)
        if re.search(r'^\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line) or \
           re.search(r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line):
            ui_indicators += 1
        
        # If it has at least 4 out of 5 UI indicators, count as UI journal
        if ui_indicators >= 4:
            ui_matches += 1
    
    return ui_matches

def detect_customer_journal_pattern(lines: list) -> int:
    """
    Detect Customer Journal pattern matches
    Pattern: timestamp tid message
    Key distinguishing features:
    - Simple format: timestamp + number + optional message
    - NO direction symbols (< > *)
    - NO square brackets [viewid]
    - NO " - " separator
    - NO "result:" or "action:" patterns
    - Often has TID numbers like 3201, 3202, 3207, 3217, 3220
    """
    customer_matches = 0
    
    for line in lines:
        line = line.strip()
        if not line or set(line) <= {'*'}:  # Skip empty lines and lines with only asterisks
            continue
        
        # Must match basic timestamp + number pattern
        basic_match = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(\d+)\s*(.*)", line)
        if not basic_match:
            continue
        
        # Count indicators that suggest this is NOT a UI journal
        non_ui_indicators = 0
        
        # 1. Should NOT have direction symbols
        if not re.search(r'\s+[<>*]\s+', line):
            non_ui_indicators += 1
        
        # 2. Should NOT have [viewid] brackets
        if not re.search(r'\[\d+\]', line):
            non_ui_indicators += 1
        
        # 3. Should NOT have " - " separator
        if ' - ' not in line:
            non_ui_indicators += 1
        
        # 4. Should NOT have result:/action: JSON pattern
        if not re.search(r'(result|action):\s*\{.*\}', line):
            non_ui_indicators += 1
        
        # 5. Bonus: Common customer journal TID numbers
        tid = basic_match.group(2)
        if tid in ['3201', '3202', '3207', '3217', '3220']:
            non_ui_indicators += 1
        
        # If it has at least 4 non-UI indicators, count as customer journal
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
        
        # Check for timestamp pattern with milliseconds
        if re.search(r'\d{2}:\d{2}:\d{2}\.\d{2}', line):
            # Check for PID pattern
            if re.search(r'PID:\w+\.\w+', line):
                # Check for Data pattern
                if 'Data:' in line:
                    matches += 1
    
    return matches

def detect_trc_error_pattern(lines: list) -> int:
    """
    Detect TRC Error pattern matches
    Pattern: AA/BB YYMMDD HH:MM:SS.MS ErrorName ModuleName PID:xxx.xxx Data:xxx
    Key distinguishing features:
    - Must have the exact AA/BB YYMMDD HH:MM:SS.MS header pattern
    - Must have PID:xxx.xxx and Data:xxx in the same line
    """
    trc_error_matches = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Primary pattern: Must match the exact TRC Error header format
        # AA/BB YYMMDD HH:MM:SS.MS ErrorName ModuleName PID:xxx.xxx Data:xxx
        trc_error_header_pattern = r'^\d{2}/\d{2}\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{1,3}\s+\w+\s+\w+\s+PID:\w+\.\w+\s+Data:\d+'
        
        if re.match(trc_error_header_pattern, line):
            trc_error_matches += 1
            continue
        
        # Secondary patterns: Look for other TRC Error specific markers
        # Check for TRC Error section headers
        if line.startswith('*** Running'):
            trc_error_matches += 1
            continue
        
        # Check for "Created by" lines that appear in TRC Error files
        if line.startswith('Created by'):
            trc_error_matches += 1
            continue
        
        # Check for Process Information header
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

def detect_file_type(file_path: str) -> str:
    """
    Main function to detect file type based on pattern matching and file extension validation
    """
    # Check if file exists
    if not Path(file_path).exists():
        return f"Error: File '{file_path}' not found"
    
    # Get file extension
    file_ext = Path(file_path).suffix.lower()
    
    # Skip common non-log file types
    if file_ext in ['.py', '.js', '.html', '.css', '.json', '.xml', '.txt', '.xlsx', '.xls', '.csv', '.pdf', '.doc', '.docx']:
        return "Unidentified: File format does not match any known patterns with sufficient confidence"
    
    # Read file content
    content = try_read_file(file_path)
    if content is None:
        return "Error: Could not read file"
    
    # Split into lines
    lines = content.split('\n')
    
    # Remove empty lines for counting
    non_empty_lines = [line for line in lines if line.strip()]
    
    # Check if we have at least 5 non-empty lines
    if len(non_empty_lines) < 5:
        return "Insufficient data: File contains less than 5 non-empty lines"
    
    # Count pattern matches for each file type
    ui_matches = detect_ui_journal_pattern(lines)
    customer_matches = detect_customer_journal_pattern(lines)
    trc_matches = detect_trc_trace_pattern(lines)
    trc_error_matches = detect_trc_error_pattern(lines)
    
    # Determine file type based on highest match count and minimum threshold
    max_matches = max(ui_matches, customer_matches, trc_matches, trc_error_matches)
    
    if max_matches < 5:
        return "Unidentified: File format does not match any known patterns with sufficient confidence"
    
    # Apply file extension validation with improved logic
    if file_ext == '.prn':
        # For .prn files, prioritize TRC Error over TRC Trace if it has substantial header matches
        # Count TRC Error header matches specifically
        trc_error_header_matches = count_trc_error_headers(lines)
        
        # If we have significant TRC Error header matches (AA/BB pattern), it's TRC Error
        if trc_error_header_matches >= 5:
            return "TRC Error (.prn)"
        # Otherwise, use the highest match count
        elif trc_error_matches == max_matches:
            return "TRC Error (.prn)"
        elif trc_matches == max_matches:
            return "TRC Trace (.prn)"
        elif trc_error_matches >= 5:
            return "TRC Error (.prn)"
        elif trc_matches >= 5:
            return "TRC Trace (.prn)"
        else:
            return "Unidentified: .prn file does not match TRC patterns with sufficient confidence"
    
    elif file_ext == '.jrn':
        if ui_matches == max_matches:
            return "UI Journal (.jrn)"
        elif customer_matches == max_matches:
            return "Customer Journal (.jrn)"
        elif ui_matches >= 5:
            return "UI Journal (.jrn)"
        elif customer_matches >= 5:
            return "Customer Journal (.jrn)"
        else:
            return "Unidentified: .jrn file does not match Journal patterns with sufficient confidence"
    
    else:
        # For files without .prn or .jrn extensions, use general matching
        # But be more restrictive about what we accept
        if trc_error_matches == max_matches and max_matches >= 10:
            return "TRC Error (.prn/.log)"
        elif ui_matches == max_matches and max_matches >= 10:
            return "UI Journal (.jrn)"
        elif customer_matches == max_matches and max_matches >= 10:
            return "Customer Journal (.jrn)"
        elif trc_matches == max_matches and max_matches >= 10:
            return "TRC Trace (.prn)"
        else:
            return "Unidentified: File format does not match any known patterns with sufficient confidence"