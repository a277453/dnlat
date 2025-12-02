"""
FAST ZIP Extraction - Optimized for Speed
Only extracts relevant DN diagnostic files
"""
import zipfile
import io
from pathlib import Path
import tempfile
import shutil
import logging
import os
import time

logger = logging.getLogger(__name__)

class ZipExtractionService:
    """
    Fast ZIP extraction - only extracts relevant files
    """
    
    def __init__(self):
        self.base_extract_path = Path(tempfile.gettempdir()) / "dn_extracts"
        self.base_extract_path.mkdir(exist_ok=True, parents=True)
        
        # ONLY these patterns will be extracted - FAST!
        self.relevant_patterns = {
            'customerjournal',
            'customer_journal', 
            'uijournal',
            'ui_journal',
            '.trc',
            'trace',
            'error',
            '.reg',
            'reg.txt',
            'registry',
            'acu',
            '.jrn',
            '.prn'
        }
        
        # Quick reject patterns - skip immediately
        self.skip_patterns = {
            '__macosx',
            '.ds_store',
            'thumbs.db',
            'desktop.ini',
            '.git',
            '.svn'
        }
    
    def is_relevant_file(self, filename: str) -> bool:
        """
        FAST check if file is relevant
        Returns True only for DN diagnostic files
        """
        filename_lower = filename.lower()
        basename = os.path.basename(filename_lower)
        
        # Skip junk immediately
        for skip in self.skip_patterns:
            if skip in filename_lower:
                return False
        
        # Skip hidden files
        if basename.startswith('.'):
            return False
        
        # Check if matches any relevant pattern
        for pattern in self.relevant_patterns:
            if pattern in filename_lower:
                return True
        
        return False
    
    def extract_zip(self, zip_content: bytes) -> Path:
        """
        FAST extraction - only extracts relevant files
        """
        if not zip_content:
            raise ValueError("Empty ZIP file")
        
        # Create extraction directory
        extract_dir = tempfile.mkdtemp(
            prefix=f"dn_{int(time.time())}_", 
            dir=self.base_extract_path
        )
        extract_path = Path(extract_dir)
        
        logger.info(f"Extracting to: {extract_path}")
        
        try:
            # Use BytesIO for speed
            with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zf:
                all_files = zf.namelist()
                logger.info(f"ZIP contains {len(all_files)} entries")
                
                # Filter ONLY relevant files - FAST
                relevant_files = [
                    f for f in all_files 
                    if not f.endswith('/') and self.is_relevant_file(f)
                ]
                
                logger.info(f"Extracting {len(relevant_files)} relevant files (skipping {len(all_files) - len(relevant_files)} irrelevant)")
                
                if not relevant_files:
                    raise ValueError("No relevant diagnostic files found in ZIP")
                
                # Extract only relevant files - FAST
                extracted = 0
                for filename in relevant_files:
                    try:
                        zf.extract(filename, extract_path)
                        extracted += 1
                    except Exception as e:
                        logger.warning(f"Skip {filename}: {e}")
                        continue
                
                logger.info(f"Extracted {extracted} files successfully")
                
                if extracted == 0:
                    raise ValueError("Failed to extract any files")
                
                return extract_path
        
        except zipfile.BadZipFile:
            shutil.rmtree(extract_path, ignore_errors=True)
            raise ValueError("Invalid ZIP file")
        except Exception as e:
            shutil.rmtree(extract_path, ignore_errors=True)
            raise Exception(f"Extraction failed: {str(e)}")
    
    def cleanup_old_extracts(self, max_age_hours: int = 24):
        """Clean up old extractions"""
        try:
            current_time = time.time()
            for extract_dir in self.base_extract_path.glob("dn_*"):
                try:
                    age = current_time - extract_dir.stat().st_mtime
                    if age > (max_age_hours * 3600):
                        shutil.rmtree(extract_dir, ignore_errors=True)
                except:
                    continue
        except:
            pass