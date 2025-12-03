from pathlib import Path
from typing import Dict, List, Optional
from .file_content_detector import detect_file_type_by_content
import os
import re
import logging

logger = logging.getLogger(__name__)

class CategorizationService:
    """
    Categorize extracted files by type using fast filename-based patterns only.
    NOTE: ACU files are extracted separately via extract_from_zip_bytes() and 
    merged into the result by the API endpoint.
    """
    
    def __init__(self):
        self.categories = {
            'customer_journals': [],
            'ui_journals': [],
            'trc_trace': [],
            'trc_error': [],
            'registry_files': [],
            'acu_files': [],
            'unidentified': []
        }
    
    def categorize_files(self, extract_path: Path, file_categories: Dict[str, List[str]], exclude_files: Optional[set] = None, mode: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Categorize all files in the extracted directory using FAST filename-based detection.
        This is for disk-extracted files only. ACU files are handled separately via extract_from_zip_bytes().
        
        Args:
            extract_path: Path to the directory containing extracted files
            file_categories: The dictionary of categories to populate (may already contain ACU files).
            exclude_files: A set of filenames to ignore during categorization (used for ACU files).
            mode: Optional processing mode. If 'registry', skips content analysis for non-registry files.
            
        Returns:
            Dictionary with categorized file lists
        """
        processed_files = set()
        
        # Ensure exclude_files is a set to prevent errors
        if exclude_files is None:
            exclude_files = set()

        logger.info(f"\nðŸ” Starting file categorization in: {extract_path}")
        logger.info(f"ðŸ“‹ Excluding {len(exclude_files)} ACU files from disk scan")
        
        # Categorize each file using FAST filename patterns only
        for file_path in extract_path.rglob("*"):
            if not file_path.is_file():
                continue
            
            if file_path.name in exclude_files:
                logger.info(f"â­ï¸  Skipping (ACU): {file_path.name}")
                continue
            
            # Skip if already processed (duplicate protection)
            if str(file_path) in processed_files:
                continue
            
            category = self._detect_category(file_path, mode=mode)
            if category and category != 'unidentified':
                file_categories[category].append(str(file_path))
                processed_files.add(str(file_path))
                logger.info(f"âœ“ [{category}] {file_path.name}")
            else:
                file_categories['unidentified'].append(str(file_path))
                processed_files.add(str(file_path))
                logger.info(f"â“ [unidentified] {file_path.name}")
        
        # Print summary
        logger.info(f"\nðŸ“Š Categorization Summary:")
        for category, files in file_categories.items():
            if files: # Only print categories that have files
                if category == 'acu_files':
                    xml_count = sum(1 for f in files if f.lower().endswith('.xml'))
                    xsd_count = sum(1 for f in files if f.lower().endswith('.xsd'))
                    logger.info(f"   {category}: {xml_count} xml / {xsd_count} xsd")
                else:
                    logger.info(f"   {category}: {len(files)} files")
        
        total = sum(len(v) for v in file_categories.values())
        logger.info(f"\nâœ“ Total: {total} files categorized\n")
        
        return file_categories
    
    def _detect_category(self, file_path: Path, mode: Optional[str] = None) -> str:
        """
        Detect file category using a hybrid approach: fast filename patterns first,
        then slower content-based detection for ambiguous cases (like .jrn and .prn files).
        ACU file detection is handled separately.
        """
        file_name_lower = file_path.name.lower()
        
        # --- 1. Registry Files (Filename-based) ---
        # This logic is working correctly and will not be changed.
        if 'reg.txt' in file_name_lower or (file_name_lower.startswith('reg') and file_name_lower.endswith('.txt')):
            return 'registry_files'

        # If in registry-only mode, classify everything else as unidentified immediately.
        if mode == 'registry':
            return 'unidentified'

        # --- 2. ACU Files (by extension) ---
        # This is a fast check for XML/XSD files which are likely ACU files.
        # The main ACU logic is in-memory, but this helps categorize any on-disk leftovers.
        if file_name_lower.endswith(('.xml', '.xsd')):
            if 'jdd' in file_name_lower or 'x3' in file_name_lower:
                return 'acu_files'

        # --- 3. Content-Based Detection for Journals and Traces ---
        # This uses the new, more robust pattern-matching logic.
        file_type = detect_file_type_by_content(str(file_path))
        
        if "Customer Journal" in file_type:
            return 'customer_journals'
        elif "UI Journal" in file_type:
            return 'ui_journals'
        elif "TRC Trace" in file_type:
            return 'trc_trace'
        elif "TRC Error" in file_type:
            return 'trc_error'

        # --- 4. Fallback ---
        # If no category is matched by filename or content, classify as unidentified.
        return 'unidentified'