from pathlib import Path
from typing import Dict, List, Optional
import os
import re
from modules.logging_config import logger
import logging


class CategorizationService:
    """
    Categorize extracted files by type using fast filename-based patterns only.
    NOTE: ACU files are extracted separately via extract_from_zip_bytes() and 
    merged into the result by the API endpoint.
    """
    
    def __init__(self):
        logger.info("CategorizationService initialized")
        self.categories = {
            'customer_journals': [],
            'ui_journals': [],
            'trc_trace': [],
            'trc_error': [],
            'registry_files': [],
            'acu_files': [],
            'unidentified': []
        }

    def categorize_files(self, extract_path: Path, file_categories: Dict[str, List[str]], exclude_files: set = None, mode: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Categorize all files in the extracted directory using FAST filename-based detection.
        """
        logger.info(f"Starting file categorization in: {extract_path}")         
        logger.debug(f"Initial exclude_files: {exclude_files}")                
        logger.debug(f"Initial file_categories keys: {list(file_categories.keys())}")

        processed_files = set()
        
        if exclude_files is None:
            exclude_files = set()

        print(f"\n🔍 Starting file categorization in: {extract_path}")
        print(f"📋 Excluding {len(exclude_files)} ACU files from disk scan")
        
        if file_categories.get('acu_files'):
            print(f"✓ Pre-loaded {len(file_categories['acu_files'])} ACU files from memory extraction")

        for file_path in extract_path.rglob("*"):
            if not file_path.is_file():
                continue
            
            if file_path.name in exclude_files:
                logger.debug(f"Skipping excluded file (ACU): {file_path.name}")
                continue
            
            if str(file_path) in processed_files:
                logger.debug(f"Skipping already processed file: {file_path.name}")
                continue
            
            category = self._detect_category(file_path, mode=mode)
            if category and category != 'unidentified':
                file_categories[category].append(str(file_path))
                processed_files.add(str(file_path))
                logger.debug(f"File categorized: {file_path.name} -> {category}")
                print(f"✓ [{category}] {file_path.name}")
            else:
                file_categories['unidentified'].append(str(file_path))
                processed_files.add(str(file_path))
                logger.debug(f"File could not be identified: {file_path.name}")
                print(f"❓ [unidentified] {file_path.name}")
        
        print(f"\n📊 Categorization Summary:")
        for category, files in file_categories.items():
            if files:
                if category == 'acu_files':
                    xml_count = sum(1 for f in files if f.lower().endswith('.xml'))
                    xsd_count = sum(1 for f in files if f.lower().endswith('.xsd'))
                    logger.info(f"{category}: {xml_count} xml / {xsd_count} xsd")
                    print(f"   {category}: {xml_count} xml / {xsd_count} xsd")
                else:
                    logger.info(f"{category}: {len(files)} files")
                    print(f"   {category}: {len(files)} files")
        
        total = sum(len(v) for v in file_categories.values())
        logger.info(f"Total files categorized: {total}")
        print(f"\n✓ Total: {total} files categorized\n")
        
        return file_categories
    
    def _detect_category(self, file_path: Path, mode: Optional[str] = None) -> str:
        file_name_lower = file_path.name.lower()
        
        if 'reg.txt' in file_name_lower or (file_name_lower.startswith('reg') and file_name_lower.endswith('.txt')):
            return 'registry_files'

        if mode == 'registry':
            return 'unidentified'

        if file_name_lower.endswith(('.jrn', '.prn')):
            file_type = self._detect_file_type_by_content(str(file_path))
            if "Customer Journal" in file_type:
                return 'customer_journals'
            if "UI Journal" in file_type:
                return 'ui_journals'
            if "TRC Trace" in file_type:
                return 'trc_trace'
            if "TRC Error" in file_type:
                return 'trc_error'
        
        return 'unidentified'

    def _detect_file_type_by_content(self, file_path: str) -> str:
        try:
            content = self._try_read_file(file_path)
            if content is None:
                logger.error(f"Failed to read file: {file_path}")
                return "Unknown (read error)"

            lines = content.split('\n')
            non_empty_lines = [line for line in lines if line.strip()]

            if len(non_empty_lines) < 5:
                return "Insufficient data"

            ui_matches = self._detect_ui_journal_pattern(lines)
            customer_matches = self._detect_customer_journal_pattern(lines)
            trc_matches = self._detect_trc_trace_pattern(lines)
            trc_error_matches = self._detect_trc_error_pattern(lines)

            max_matches = max(ui_matches, customer_matches, trc_matches, trc_error_matches)

            if max_matches < 5:
                return "Unidentified"

            file_ext = Path(file_path).suffix.lower()

            if file_ext == '.prn':
                trc_error_header_matches = self._count_trc_error_headers(lines)
                if trc_error_header_matches >= 5: return "TRC Error"
                elif trc_error_matches == max_matches: return "TRC Error"
                elif trc_matches == max_matches: return "TRC Trace"
                else: return "Unidentified"
            
            elif file_ext == '.jrn':
                if ui_matches == max_matches: return "UI Journal"
                elif customer_matches == max_matches: return "Customer Journal"
                else: return "Unidentified"
            
        except (IOError, OSError) as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return "Unknown (access error)"

        return "Unknown"

    def _try_read_file(self, filepath: str) -> Optional[str]:
        encodings = ['utf-8', 'latin1', 'windows-1252', 'utf-16']
        
        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
                    return f.read()
            except Exception as e:
                logger.debug(f"Failed to read {filepath} with encoding {encoding}: {e}")
        
        try:
            with open(filepath, 'rb') as f:
                return f.read().decode('utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to read file {filepath} in binary mode: {e}")
            return None

    # --- Pattern detection methods remain unchanged ---
    def _detect_ui_journal_pattern(self, lines: list) -> int:
        ui_matches = 0
        for line in lines:
            line = line.strip()
            if not line: continue
            ui_indicators = 0
            if re.search(r'\s+[<>*]\s+', line): ui_indicators += 1
            if re.search(r'\[\d+\]', line): ui_indicators += 1
            if ' - ' in line: ui_indicators += 1
            if re.search(r'(result|action):\s*\{.*\}', line): ui_indicators += 1
            if re.search(r'^\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line) or \
               re.search(r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s+\d+\s+\w+\s+[<>*]', line):
                ui_indicators += 1
            if ui_indicators >= 4: ui_matches += 1
        return ui_matches

    def _detect_customer_journal_pattern(self, lines: list) -> int:
        customer_matches = 0
        for line in lines:
            line = line.strip()
            if not line or set(line) <= {'*'}: continue
            basic_match = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(\d+)\s*(.*)", line)
            if not basic_match: continue
            non_ui_indicators = 0
            if not re.search(r'\s+[<>*]\s+', line): non_ui_indicators += 1
            if not re.search(r'\[\d+\]', line): non_ui_indicators += 1
            if ' - ' not in line: non_ui_indicators += 1
            if not re.search(r'(result|action):\s*\{.*\}', line): non_ui_indicators += 1
            tid = basic_match.group(2)
            if tid in ['3201', '3202', '3207', '3217', '3220']: non_ui_indicators += 1
            if non_ui_indicators >= 4: customer_matches += 1
        return customer_matches

    def _detect_trc_trace_pattern(self, lines: list) -> int:
        matches = 0
        for line in lines:
            line = line.strip()
            if not line: continue
            if re.search(r'\d{2}:\d{2}:\d{2}\.\d{2}', line):
                if re.search(r'PID:\w+\.\w+', line):
                    if 'Data:' in line:
                        matches += 1
        return matches

    def _detect_trc_error_pattern(self, lines: list) -> int:
        trc_error_matches = 0
        for line in lines:
            line = line.strip()
            if not line: continue
            trc_error_header_pattern = r'^\d{2}/\d{2}\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{1,3}\s+\w+\s+\w+\s+PID:\w+\.\w+\s+Data:\d+'
            if re.match(trc_error_header_pattern, line): trc_error_matches += 1
            elif line.startswith('*** Running'): trc_error_matches += 1
            elif line.startswith('Created by'): trc_error_matches += 1
            elif line == 'Process Information:': trc_error_matches += 1
        return trc_error_matches

    def _count_trc_error_headers(self, lines: list) -> int:
        header_matches = 0
        trc_error_header_pattern = r'^\d{2}/\d{2}\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{1,3}\s+\w+\s+\w+\s+PID:\w+\.\w+\s+Data:\d+'
        for line in lines:
            line = line.strip()
            if not line: continue
            if re.match(trc_error_header_pattern, line): header_matches += 1
        return header_matches
