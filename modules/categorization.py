from pathlib import Path
from typing import Dict, List
from .configManager import detect_file_type

class CategorizationService:
    """
    Step 3: Categorize extracted files by type
    """
    def __init__(self):
        self.categories = {
            'customer_journals': [],
            'ui_journals': [],
            'trc_trace': [],
            'trc_error': [],
            'registry_files': []
        }

    def categorize_files(self, extract_path: Path) -> Dict[str, List[str]]:
        """
        Categorize all files in the extracted directory.
        Args:
            extract_path: Path to the directory containing extracted files
        Returns:
            Dictionary with categorized file lists
        """
        # Find all files
        all_files = list(extract_path.rglob("*"))
        
        # Initialize fresh categories
        file_categories = {
            'customer_journals': [],
            'ui_journals': [],
            'trc_trace': [],
            'trc_error': [],
            'registry_files': []
        }
        
        # Categorize each file
        for file_path in all_files:
            if file_path.is_file():
                category = self._detect_category(file_path)
                if category:
                    file_categories[category].append(str(file_path))
        
        return file_categories

    def _detect_category(self, file_path: Path) -> str:
        """
        Detect which category a file belongs to.
        """
        file_name_lower = file_path.name.lower()
        
        # DEBUG
        print(f"🔍 Checking: {file_path.name} (lowercase: {file_name_lower})")
        
        # Check for .reg
        if file_name_lower.endswith('.reg'):
            print(f"  ✅ Matched .reg")
            return 'registry_files'
        
        # Check for files with 'reg' in name ending in .txt
        if file_name_lower.endswith('.txt') and 'reg' in file_name_lower:
            print(f"  ✅ Matched .txt with reg")
            return 'registry_files'
        
        # Other files
        file_type = detect_file_type(str(file_path))
        print(f"  📄 Type: {file_type}")
        
        if "Customer Journal" in file_type:
            return 'customer_journals'
        elif "UI Journal" in file_type:
            return 'ui_journals'
        elif "TRC Trace" in file_type:
            return 'trc_trace'
        elif "TRC Error" in file_type:
            return 'trc_error'
        
        print(f"  ❌ No match")
        return None