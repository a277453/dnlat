import xmltodict  # type: ignore
import re
from pathlib import Path
from typing import Optional
from modules.logging_config import logger
import logging

logger.info("Starting config_parser")
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
    logger.info(f"Parsing XML file: {xml_file}")
    try:
        with open(xml_file, 'r', encoding='utf-8') as file:
            txn_xml = file.read()
    except Exception as e:
        logger.error(f"Failed to read XML file {xml_file}: {e}")
        raise

    try:
        config_dict = xmltodict.parse(txn_xml)
    except Exception as e:
        logger.error(f"Failed to parse XML content from {xml_file}: {e}")
        raise
    
    logger.debug("Extracting transaction mappings from XML")
    real_name = {
        txn['key']: txn['value']
        for txn in config_dict['configuration']['transactionList']['transaction']
    }
    logger.info(f"Extracted {len(real_name)} transaction mappings")
    
    start_time_list = config_dict['configuration']['customerJournalParsing']['starttransaction'].split(',')
    start_time_list = [tid.strip() for tid in start_time_list]  # Remove whitespace
    logger.debug(f"Start transaction TIDs: {start_time_list}")
    
    end_time_list = config_dict['configuration']['customerJournalParsing']['endtransaction'].split(',')
    end_time_list = [tid.strip() for tid in end_time_list]  # Remove whitespace
    logger.debug(f"End transaction TIDs: {end_time_list}")
    
    chain_time_list = []
    try:
        chaining_element = config_dict['configuration']['customerJournalParsing'].get('chainingtransaction', '')
        if chaining_element:
            chain_time_list = chaining_element.split(',')
            chain_time_list = [tid.strip() for tid in chain_time_list if tid.strip()]  # Remove whitespace and empty strings
    except (KeyError, AttributeError):
        chain_time_list = []
    
    logger.info(f"Chain transaction TIDs: {chain_time_list if chain_time_list else 'None configured'}")
    
    return real_name, start_time_list, end_time_list, chain_time_list


def validate_xml_config(xml_file):
    """
    Validates that the XML configuration file has all required sections.
    
    Args:
        xml_file (str): Path to the XML file
        
    Returns:
        dict: Validation results with status and any missing sections
    """
    logger.info(f"Validating XML configuration: {xml_file}")
    try:
        with open(xml_file, 'r', encoding='utf-8') as file:
            txn_xml = file.read()
        
        config_dict = xmltodict.parse(txn_xml)
        
        validation_result = {
            'valid': True,
            'missing_sections': [],
            'warnings': []
        }
        
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
                logger.error(f"Missing required XML section: {section}")
        
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
                logger.warning(f"Optional XML section missing: {section}")
        
        logger.info("XML validation completed")
        return validation_result
        
    except Exception as e:
        logger.error(f"Failed to validate XML file {xml_file}: {e}")
        return {
            'valid': False,
            'error': f"Failed to parse XML: {str(e)}",
            'missing_sections': [],
            'warnings': []
        }


def get_all_tids(xml_file):
    """
    Convenience function to get all TID lists with descriptive names.
    
    Args:
        xml_file (str): Path to the XML file
        
    Returns:
        dict: Dictionary containing all TID lists
    """
    logger.info(f"Getting all TID lists from XML: {xml_file}")
    real_name, start_tids, end_tids, chain_tids = xml_to_dict(xml_file)
    
    all_tids = {
        'transaction_names': real_name,
        'start_tids': start_tids,
        'end_tids': end_tids,
        'chain_tids': chain_tids,
        'all_parsing_tids': start_tids + end_tids + chain_tids
    }
    logger.debug(f"All TID lists extracted: {all_tids}")
    return all_tids


def debug_print_config(xml_file):
    """
    Debug function to print the parsed configuration.
    
    Args:
        xml_file (str): Path to the XML file
    """
    logger.info(f"Debug printing configuration from XML: {xml_file}")
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
        logger.info("Debug print completed")
    except Exception as e:
        logger.error(f"Error debugging configuration: {e}")


# Optional: Function to update XML configuration programmatically
def try_read_file(filepath: str) -> Optional[str]:
    """Try to read file with different encodings"""
    logger.info(f"Attempting to read file: {filepath}")
    encodings = ['utf-8', 'latin1', 'windows-1252', 'utf-16']
    
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
                content = f.read()
                logger.debug(f"File read successfully with encoding: {encoding}")
                return content
        except Exception:
            continue
    
    try:
        with open(filepath, 'rb') as f:
            content = f.read().decode('utf-8', errors='ignore')
            logger.debug("File read successfully in binary mode with UTF-8 decode")
            return content
    except Exception as e:
        logger.error(f"Failed to read file {filepath}: {e}")
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
        if re.search(r'\s+[<>*]\s+', line):
            ui_indicators += 1
        if re.search(r'\[\d+\]', line):
            ui_indicators += 1
        if ' - ' in line:
            ui_indicators += 1
        if re.search(r'(result|action):\s*\{.*\}', line):
            ui_indicators += 1
        if re.search(r'^\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line) or \
           re.search(r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line):
            ui_indicators += 1
        
        if ui_indicators >= 4:
            ui_matches += 1
    
    logger.debug(f"UI Journal pattern matches: {ui_matches}")
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
        if not re.search(r'\s+[<>*]\s+', line):
            non_ui_indicators += 1
        if not re.search(r'\[\d+\]', line):
            non_ui_indicators += 1
        if ' - ' not in line:
            non_ui_indicators += 1
        if not re.search(r'(result|action):\s*\{.*\}', line):
            non_ui_indicators += 1
        
        tid = basic_match.group(2)
        if tid in ['3201', '3202', '3207', '3217', '3220']:
            non_ui_indicators += 1
        
        if non_ui_indicators >= 4:
            customer_matches += 1
    
    logger.debug(f"Customer Journal pattern matches: {customer_matches}")
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
        if re.search(r'\d{2}:\d{2}:\d{2}\.\d{2}', line) and \
           re.search(r'PID:\w+\.\w+', line) and 'Data:' in line:
            matches += 1
    logger.debug(f"TRC Trace pattern matches: {matches}")
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
        if line.startswith('*** Running') or line.startswith('Created by') or line == 'Process Information:':
            trc_error_matches += 1
            continue
    logger.debug(f"TRC Error pattern matches: {trc_error_matches}")
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
    logger.debug(f"TRC Error header matches: {header_matches}")
    return header_matches


def detect_file_type(file_path: str) -> str:
    """
    Main function to detect file type based on pattern matching and file extension validation
    """
    logger.info(f"Detecting file type for: {file_path}")
    
    if not Path(file_path).exists():
        logger.error(f"File not found: {file_path}")
        return f"Error: File '{file_path}' not found"
    
    file_ext = Path(file_path).suffix.lower()
    
    if file_ext in ['.py', '.js', '.html', '.css', '.json', '.xml', '.txt', '.xlsx', '.xls', '.csv', '.pdf', '.doc', '.docx']:
        logger.info(f"File extension '{file_ext}' not suitable for pattern detection")
        return "Unidentified: File format does not match any known patterns with sufficient confidence"
    
    content = try_read_file(file_path)
    if content is None:
        logger.error(f"Could not read file: {file_path}")
        return "Error: Could not read file"
    
    lines = content.split('\n')
    non_empty_lines = [line for line in lines if line.strip()]
    
    if len(non_empty_lines) < 5:
        logger.warning("File contains less than 5 non-empty lines")
        return "Insufficient data: File contains less than 5 non-empty lines"
    
    ui_matches = detect_ui_journal_pattern(lines)
    customer_matches = detect_customer_journal_pattern(lines)
    trc_matches = detect_trc_trace_pattern(lines)
    trc_error_matches = detect_trc_error_pattern(lines)
    
    max_matches = max(ui_matches, customer_matches, trc_matches, trc_error_matches)
    logger.debug(f"Pattern match counts - UI: {ui_matches}, Customer: {customer_matches}, TRC Trace: {trc_matches}, TRC Error: {trc_error_matches}")

    if max_matches < 5:
        logger.info("File pattern match insufficient for identification")
        return "Unidentified: File format does not match any known patterns with sufficient confidence"
    
    # Apply file extension validation with improved logic
    if file_ext == '.prn':
        trc_error_header_matches = count_trc_error_headers(lines)
        
        if trc_error_header_matches >= 5:
            logger.info("Detected: TRC Error (.prn)")
            return "TRC Error (.prn)"
        elif trc_error_matches == max_matches:
            logger.info("Detected: TRC Error (.prn)")
            return "TRC Error (.prn)"
        elif trc_matches == max_matches:
            logger.info("Detected: TRC Trace (.prn)")
            return "TRC Trace (.prn)"
        elif trc_error_matches >= 5:
            logger.info("Detected: TRC Error (.prn)")
            return "TRC Error (.prn)"
        elif trc_matches >= 5:
            logger.info("Detected: TRC Trace (.prn)")
            return "TRC Trace (.prn)"
        else:
            logger.info("Unidentified .prn file")
            return "Unidentified: .prn file does not match TRC patterns with sufficient confidence"
    
    elif file_ext == '.jrn':
        if ui_matches == max_matches:
            logger.info("Detected: UI Journal (.jrn)")
            return "UI Journal (.jrn)"
        elif customer_matches == max_matches:
            logger.info("Detected: Customer Journal (.jrn)")
            return "Customer Journal (.jrn)"
        elif ui_matches >= 5:
            logger.info("Detected: UI Journal (.jrn)")
            return "UI Journal (.jrn)"
        elif customer_matches >= 5:
            logger.info("Detected: Customer Journal (.jrn)")
            return "Customer Journal (.jrn)"
        else:
            logger.info("Unidentified .jrn file")
            return "Unidentified: .jrn file does not match Journal patterns with sufficient confidence"
    
    else:
        if trc_error_matches == max_matches and max_matches >= 10:
            logger.info("Detected: TRC Error (.prn/.log)")
            return "TRC Error (.prn/.log)"
        elif ui_matches == max_matches and max_matches >= 10:
            logger.info("Detected: UI Journal (.jrn)")
            return "UI Journal (.jrn)"
        elif customer_matches == max_matches and max_matches >= 10:
            logger.info("Detected: Customer Journal (.jrn)")
            return "Customer Journal (.jrn)"
        elif trc_matches == max_matches and max_matches >= 10:
            logger.info("Detected: TRC Trace (.prn)")
            return "TRC Trace (.prn)"
        else:
            logger.info("Unidentified: insufficient pattern confidence")
            return "Unidentified: File format does not match any known patterns with sufficient confidence"


if __name__ == "__main__":
    # Test the configuration parser
    xml_file = '/Users/yuvikaagrawal/Desktop/DN/ML_DN/dnLogAtConfig.xml'
    
    try:
        debug_print_config(xml_file)
        
        # Validate configuration
        validation = validate_xml_config(xml_file)
        if validation['valid']:
            print("\n✅ XML Configuration is valid!")
            if validation['warnings']:
                print("⚠️  Warnings:")
                for warning in validation['warnings']:
                    print(f"  - {warning}")
        else:
            print("\n❌ XML Configuration has issues:")
            for missing in validation['missing_sections']:
                print(f"  - Missing: {missing}")
                
    except Exception as e:
        print(f"Error testing configuration: {e}")