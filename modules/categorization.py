from pathlib import Path
from typing import Dict, List, Optional
import os
import re
from modules.logging_config import logger
import logging


class CategorizationService:
    """
    Categorize extracted files by type using folder hierarchy and filename patterns.
    NOTE: ACU files are extracted separately via extract_from_zip_bytes() and
    merged into the result by the API endpoint.
    """

    def __init__(self):
        """
        FUNCTION: __init__

        DESCRIPTION:
            Initialize CategorizationService and sets up default categories.

        USAGE:
            service = CategorizationService()

        PARAMETERS:
            None

        RETURNS:
            None

        RAISES:
            None
        """
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

    def categorize_files(
        self,
        extract_path: Path,
        file_categories: Dict[str, List[str]],
        exclude_files: set = None,
        mode: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """
        FUNCTION: categorize_files

        DESCRIPTION:
            Categorize all files in a directory using fast filename-based detection.

        USAGE:
            categorized = service.categorize_files(Path("/tmp/extracted"), categories, exclude_files=set(), mode="registry")

        PARAMETERS:
            extract_path (Path)             : Directory path containing extracted files
            file_categories (dict)          : Pre-existing dictionary of categorized files
            exclude_files (set, optional)   : Set of filenames to exclude from categorization
            mode (str, optional)            : Mode of categorization (e.g., 'registry')

        RETURNS:
            dict : Updated dictionary of categorized files

        RAISES:
            None
        """
        logger.info(f"Starting file categorization in: {extract_path}")
        logger.debug(f"Initial exclude_files: {exclude_files}")
        logger.debug(f"Initial file_categories keys: {list(file_categories.keys())}")

        processed_files = set()

        if exclude_files is None:
            exclude_files = set()

        print(f"\n[INFO] Starting file categorization in: {extract_path}")
        print(f"[INFO] Excluding {len(exclude_files)} ACU files from disk scan")

        if file_categories.get('acu_files'):
            print(f"[OK] Pre-loaded {len(file_categories['acu_files'])} ACU files from memory extraction")
            logger.info("Pre-loaded ACU files from memory extraction")

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
                print(f"[OK] [{category}] {file_path.name}")
            else:
                file_categories['unidentified'].append(str(file_path))
                processed_files.add(str(file_path))
                logger.debug(f"File could not be identified: {file_path.name}")

        logger.info("Categorization Summary:")
        print("\n[INFO] Categorization Summary:")

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
        print(f"\n[OK] Total: {total} files categorized\n")

        return file_categories

    def _detect_category(self, file_path: Path, mode: Optional[str] = None) -> str:
        """
        FUNCTION: _detect_category

        DESCRIPTION:
            Detects the category of a given file based on filename and content.

        USAGE:
            category = service._detect_category(Path("/tmp/file.jrn"), mode=None)

        PARAMETERS:
            file_path (Path)   : Path of the file to categorize
            mode (str, optional) : Special mode affecting categorization

        RETURNS:
            str : File category (e.g., 'customer_journals', 'unidentified')

        RAISES:
            None
        """
        file_name_lower = file_path.name.lower()
        all_parents = [p.lower() for p in file_path.parts]
        normalized_path = str(file_path).replace('\\', '/').lower()

        logger.debug(f"Analyzing file: {file_path.name}")
        logger.debug(f"Path hierarchy: {' > '.join(file_path.parts[:-1])}")
        logger.debug(f"Normalized path: {normalized_path}")

        # === REGISTRY FILES ===
        has_registry_folder = any('registry' in parent or 'reg' in parent for parent in all_parents)

        if has_registry_folder:
            if file_name_lower.endswith(('.reg', '.txt', '.ini', '.cfg', '.conf')):
                logger.debug("MATCH: Found in REGISTRY folder -> registry_files")
                return 'registry_files'

        if file_name_lower.endswith('.reg'):
            logger.debug("MATCH: .reg extension -> registry_files")
            return 'registry_files'

        if file_name_lower.endswith('.txt') and 'reg' in file_name_lower:
            logger.debug("MATCH: .txt with 'reg' in name -> registry_files")
            return 'registry_files'

        if mode == 'registry':
            return 'unidentified'

        # === CUSTOMER JOURNALS ===
        has_customer_folder = any(
            'customer' in parent and ('journal' in parent or parent == 'customer')
            for parent in all_parents
        )

        if has_customer_folder and file_name_lower.endswith('.jrn'):
            logger.debug("MATCH: Found in CUSTOMER folder -> customer_journals")
            return 'customer_journals'

        # === UI JOURNALS ===
        has_ui_folder = any('ui' in parent and 'journal' in parent for parent in all_parents)

        if has_ui_folder and file_name_lower.endswith('.jrn'):
            logger.debug("MATCH: Found in UI JOURNAL folder -> ui_journals")
            return 'ui_journals'

        # === TRC TRACE FILES ===
        has_trace_folder = any('trace' in parent for parent in all_parents)

        if has_trace_folder and file_name_lower.endswith('.prn'):
            logger.debug("MATCH: Found in TRACE folder -> trc_trace")
            return 'trc_trace'

        # === TRC ERROR FILES ===
        has_error_folder = any('error' in parent for parent in all_parents)

        if has_error_folder and file_name_lower.endswith('.prn'):
            logger.debug("MATCH: Found in ERROR folder -> trc_error")
            return 'trc_error'

        # === CONTENT-BASED DETECTION ===
        if file_name_lower.endswith(('.jrn', '.prn')):
            logger.debug("Running content-based detection...")
            file_type = self._detect_file_type_by_content(str(file_path))
            logger.debug(f"Content type detected: {file_type}")

            if "Customer Journal" in file_type:
                if not has_ui_folder:
                    return 'unidentified'
            elif "UI Journal" in file_type:
                return 'ui_journals'
            elif "TRC Trace" in file_type:
                return 'trc_trace'
            elif "TRC Error" in file_type:
                return 'trc_error'

        logger.debug("NO MATCH: File will be unidentified")
        return 'unidentified'

    def _detect_file_type_by_content(self, file_path: str) -> str:
        """
        FUNCTION: _detect_file_type_by_content

        DESCRIPTION:
            Detect the type of a file by analyzing its content lines and patterns.

        USAGE:
            file_type = service._detect_file_type_by_content("/tmp/file.jrn")

        PARAMETERS:
            file_path (str) : Path to the file being analyzed

        RETURNS:
            str : File type detected (e.g., 'Customer Journal', 'UI Journal', 'TRC Error', 'Unidentified')

        RAISES:
            None
        """
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
        """
        FUNCTION: _try_read_file

        DESCRIPTION:
            Attempt to read a file using multiple encodings and fallback to binary read if needed.

        USAGE:
            content = service._try_read_file("/tmp/file.jrn")

        PARAMETERS:
            filepath (str) : Path to the file to read

        RETURNS:
            Optional[str] : File content if successful, None if unable to read

        RAISES:
            None
        """
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
        """
        FUNCTION: _detect_ui_journal_pattern

        DESCRIPTION:
            Count occurrences of UI Journal patterns in file lines.

        USAGE:
            matches = service._detect_ui_journal_pattern(lines)

        PARAMETERS:
            lines (list) : List of file lines to analyze

        RETURNS:
            int : Number of lines matching UI Journal pattern

        RAISES:
            None
        """
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
        """
        FUNCTION: _detect_customer_journal_pattern

        DESCRIPTION:
            Count occurrences of Customer Journal patterns in file lines.

        USAGE:
            matches = service._detect_customer_journal_pattern(lines)

        PARAMETERS:
            lines (list) : List of file lines to analyze

        RETURNS:
            int : Number of lines matching Customer Journal pattern

        RAISES:
            None
        """
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
        """
        FUNCTION: _detect_trc_trace_pattern

        DESCRIPTION:
            Count occurrences of TRC Trace patterns in file lines.

        USAGE:
            matches = service._detect_trc_trace_pattern(lines)

        PARAMETERS:
            lines (list) : List of file lines to analyze

        RETURNS:
            int : Number of lines matching TRC Trace pattern

        RAISES:
            None
        """
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
        """
        FUNCTION: _detect_trc_error_pattern

        DESCRIPTION:
            Count occurrences of TRC Error patterns in file lines.

        USAGE:
            matches = service._detect_trc_error_pattern(lines)

        PARAMETERS:
            lines (list) : List of file lines to analyze

        RETURNS:
            int : Number of lines matching TRC Error pattern

        RAISES:
            None
        """
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
        """
        FUNCTION: _count_trc_error_headers

        DESCRIPTION:
            Count TRC Error header lines in file content.

        USAGE:
            count = service._count_trc_error_headers(lines)

        PARAMETERS:
            lines (list) : List of file lines to analyze

        RETURNS:
            int : Number of TRC Error headers found

        RAISES:
            None
        """
        header_matches = 0
        trc_error_header_pattern = r'^\d{2}/\d{2}\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{1,3}\s+\w+\s+\w+\s+PID:\w+\.\w+\s+Data:\d+'
        for line in lines:
            line = line.strip()
            if not line: continue
            if re.match(trc_error_header_pattern, line): header_matches += 1
        return header_matches
