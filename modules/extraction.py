"""
FAST ZIP Extraction - Optimized for Speed
Extracts relevant DN diagnostic files and ACU configuration files.
"""
import zipfile
import io
from pathlib import Path
import tempfile
import shutil
import logging
import os
import time
from typing import Dict, List, Tuple, Union
import struct
import html
import zlib
from modules.logging_config import logger


logger.info("Extraction service initialized")

# Low-level ZIP parsing constants
CD_SIG = b'PK\x01\x02'
LH_SIG = b'PK\x03\x04'

class ZipExtractionService:
    """
    CLASS: ZipExtractionService

    DESCRIPTION:
        Fast ZIP extraction service that extracts only relevant DN diagnostic files.
        Handles nested ZIPs recursively and removes irrelevant files or empty directories.
    
    USAGE:
        service = ZipExtractionService()
        extract_path = service.extract_zip(zip_bytes)
        service.cleanup_old_extracts(max_age_hours=24)
    
    PARAMETERS:
        None
    
    RETURNS:
        None
    
    RAISES:
        None
    """
    
    def __init__(self):
        """
        FUNCTION: __init__

        DESCRIPTION:
            Initialize ZipExtractionService, setting base extraction path, relevant file patterns, and skip patterns.
        
        USAGE:
            service = ZipExtractionService()
        
        PARAMETERS:
            None
        
        RETURNS:
            None
        
        RAISES:
            None
        """
        
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
            '.jrn', # Journal files
            '.prn', # Trace files
            'jdd',  # ACU files
            'x3',   # ACU files
            '.zip'  # CRITICAL: To extract nested archives
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

        logger.debug(f"extraction base path: {self.base_extract_path}")
        logger.debug(f"relevant patterns: {self.relevant_patterns}")
        logger.debug(f"skip patterns: {self.skip_patterns}")
        logger.info(f"ZipExtractionService initialized at {self.base_extract_path}")

    def is_relevant_file(self, filename: str) -> bool:
        """
        FUNCTION: is_relevant_file

        DESCRIPTION:
            Checks if a given file is relevant for DN diagnostics based on predefined patterns and skip rules.
        
        USAGE:
            result = service.is_relevant_file("path/to/file.xml")
        
        PARAMETERS:
            filename (str) : Full filename or relative path of the file
        
        RETURNS:
            bool : True if the file is relevant, False otherwise
        
        RAISES:
            None
        """
        filename_lower = filename.lower()
        basename = os.path.basename(filename_lower)
        
        # Skip junk immediately
        for skip in self.skip_patterns:
            if skip in filename_lower:
                logger.debug(f"Skipping irrelevant file (skip pattern match): {filename}")
                return False
        
        # Skip hidden files
        if basename.startswith('.'):
            logger.debug(f"Skipping hidden file: {filename}") 
            return False
        
        # Check if matches any relevant pattern
        for pattern in self.relevant_patterns:
            if pattern in filename_lower:
                logger.debug(f"File is relevant: {filename}")
                return True
        
        
        logger.debug(f"File does not match relevant patterns: {filename}")
        return False
    
    def _extract_nested_zips(self, extract_path: Path):
        """
        FUNCTION: _extract_nested_zips

        DESCRIPTION:
            Recursively extracts nested ZIP files found in a given extraction directory.
            Removes nested ZIP files after successful extraction.
        
        USAGE:
            service._extract_nested_zips(Path("/tmp/extract_dir"))
        
        PARAMETERS:
            extract_path (Path) : Path object pointing to the extraction directory
        
        RETURNS:
            None
        
        RAISES:
            None
        """
        processed_zips = set()
        # Use a while loop to handle multiple layers of nesting
        while True:
            # Find all .zip files that have not been processed yet
            nested_zips = [
                p for p in extract_path.rglob("*.zip") 
                if p.is_file() and p not in processed_zips
            ]
            
            if not nested_zips:
                logger.debug("No more nested ZIP files found to extract.")
                break  # No more new zips to extract

            for zip_path in nested_zips:
                processed_zips.add(zip_path)  # Mark as processed immediately
                logger.info(f"📦 Found nested ZIP: {zip_path.relative_to(self.base_extract_path)}. Extracting...")
                
                # Define a subdirectory for the contents of the nested ZIP to avoid name clashes
                nested_extract_dir = zip_path.parent / zip_path.stem
                nested_extract_dir.mkdir(exist_ok=True)

                try:
                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        zf.extractall(nested_extract_dir)
                        logger.debug(f"Nested ZIP extracted to {nested_extract_dir}")
                    zip_path.unlink()  # Remove the nested ZIP file after successful extraction
                    logger.info(f"Removed nested ZIP file {zip_path.name} after extraction")
                except (zipfile.BadZipFile, Exception) as e:
                    logger.error(f"  ❌ Error extracting nested ZIP {zip_path.name}: {e}. Leaving file as is.")

    def extract_zip(self, zip_content: bytes) -> Path:
        """
        FUNCTION: extract_zip

        DESCRIPTION:
            Extracts all files from a ZIP archive into a temporary directory and filters only relevant files.
            Handles nested ZIPs after extraction.
        
        USAGE:
            extract_path = service.extract_zip(zip_bytes)
        
        PARAMETERS:
            zip_content (bytes) : Bytes of the ZIP archive
        
        RETURNS:
            Path : Path to the directory containing the extracted relevant files
        
        RAISES:
            ValueError : When ZIP content is empty or contains no relevant files
            Exception   : For general extraction errors
        """

        if not zip_content:
            logger.warning("Empty ZIP content received")
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
                
                # NEW STRATEGY: Extract ALL files first
                # This avoids path separator mismatch issues
                logger.info("Extracting all files to avoid path separator issues...")
                zf.extractall(extract_path)
                logger.info(f"Extraction complete. Now filtering for relevant files...")
                
                # Now walk the extracted directory and remove irrelevant files
                extracted = 0
                removed = 0
                
                for root, dirs, files in os.walk(extract_path):
                    for file in files:
                        file_path = Path(root) / file
                        relative_path = file_path.relative_to(extract_path)
                        
                        if self.is_relevant_file(str(relative_path)):
                            extracted += 1
                        else:
                            # Remove irrelevant file
                            try:
                                file_path.unlink()
                                removed += 1
                                logger.debug(f"Removed irrelevant file: {relative_path}") 
                            except:
                                logger.warning(f"Failed to remove irrelevant file: {relative_path}") 
                
                # Clean up empty directories
                for root, dirs, files in os.walk(extract_path, topdown=False):
                    for dir_name in dirs:
                        dir_path = Path(root) / dir_name
                        try:
                            if not any(dir_path.iterdir()):
                                logger.debug(f"Removed empty directory: {dir_path}")
                                dir_path.rmdir()
                        except:
                            logger.warning(f"Failed to remove empty directory: {dir_path}")
                
                logger.info(f"Kept {extracted} relevant files, removed {removed} irrelevant files")
                
                if extracted == 0:
                    logger.error("No relevant diagnostic files found in ZIP") 
                    raise ValueError("No relevant diagnostic files found in ZIP")

                # --- NEW: Handle nested ZIPs ---
                logger.debug(f"handle nested zip in extraction path: {extract_path}")
                self._extract_nested_zips(extract_path)
                
                return extract_path
        
        except zipfile.BadZipFile:
            logger.error(f"Bad ZIP file: extraction failed for {extract_path}") 
            shutil.rmtree(extract_path, ignore_errors=True)
            raise ValueError("Invalid ZIP file")
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            shutil.rmtree(extract_path, ignore_errors=True)
            raise Exception(f"Extraction failed: {str(e)}")
    
    def cleanup_old_extracts(self, max_age_hours: int = 24):
        """
        FUNCTION: cleanup_old_extracts

        DESCRIPTION:
            Deletes old extraction directories older than the specified age to save disk space.
        
        USAGE:
            service.cleanup_old_extracts(max_age_hours=48)
        
        PARAMETERS:
            max_age_hours (int) : Maximum age (in hours) of extraction directories to keep
        
        RETURNS:
            None
        
        RAISES:
            None
        """
        try:
            current_time = time.time()
            for extract_dir in self.base_extract_path.glob("dn_*"):
                try:
                    age = current_time - extract_dir.stat().st_mtime
                    if age > (max_age_hours * 3600):
                        shutil.rmtree(extract_dir, ignore_errors=True)
                        logger.info(f"cleaning up old extraction")

                except:
                    continue
        except:
            logger.error(f"error cleaning up old extractions")
            

# --- ACU Parser Specific Extraction Logic ---

def _decode_bytes_to_text(b: bytes) -> str:
    """
    FUNCTION: _decode_bytes_to_text

    DESCRIPTION:
        Decodes byte content to text using UTF-8 encoding first, then falls back to Latin1 if needed.
    
    USAGE:
        text = _decode_bytes_to_text(byte_content)
    
    PARAMETERS:
        b (bytes) : Byte sequence to decode
    
    RETURNS:
        str : Decoded text
    
    RAISES:
        None
    """
    try:
        logger.debug("Decoding bytes as utf-8")
        return b.decode('utf-8')
    except Exception:
        return b.decode('latin1', errors='replace')

def render_html_documentation(html_string: str) -> str:
    """
    FUNCTION: render_html_documentation

    DESCRIPTION:
        Converts HTML content into readable plain text with simple formatting.
    
    USAGE:
        text = render_html_documentation("<p>Hello <b>World</b></p>")
    
    PARAMETERS:
        html_string (str) : HTML content as string
    
    RETURNS:
        str : Formatted text with HTML tags replaced by newlines or markdown
    
    RAISES:
        None
    """
    if not html_string or '<' not in html_string:
        return html_string

    # First, decode HTML entities like &lt; into 
    processed_text = html.unescape(html_string)

    # Define replacements for HTML tags
    replacements = {
        "<p>": "", "</p>": "\n\n",
        "<b>": "**", "</b>": "**",
        "<i>": "*", "</i>": "*",
        "<br>": "\n", "<br/>": "\n", "<br />": "\n"
    }

    # Apply all replacements
    for old, new in replacements.items():
        processed_text = processed_text.replace(old, new)

    # Clean up leading/trailing whitespace and excessive newlines
    lines = [line.strip() for line in processed_text.strip().split('\n')]
    return "\n".join(line for line in lines if line)

def extract_from_zip_bytes(zip_content: bytes, logs: List[str], target_prefixes: Tuple[str, ...] = ('jdd', 'x3')) -> Dict[str, str]:
    """
    FUNCTION: extract_from_zip_bytes

    DESCRIPTION:
        Low-level ZIP extraction from bytes.
        Extracts target XML/XSD files and nested ZIPs recursively.
        Uses binary parsing to handle edge cases that zipfile module may fail on.
    
    USAGE:
        files = extract_from_zip_bytes(zip_bytes, logs, target_prefixes=('jdd', 'x3'))
    
    PARAMETERS:
        zip_content (bytes)                  : Bytes of the ZIP archive
        logs (List[str])                     : List to append extraction log messages
        target_prefixes (Tuple[str, ...])    : File prefixes to filter for XML/XSD files
    
    RETURNS:
        dict : Mapping of filenames to file content. XSD files are stored with '__xsd__' prefix
    
    RAISES:
        ValueError : When ZIP is empty or contains no relevant files
        Exception   : For general parsing or decompression errors
    """
    logger.info(f"starting low-level zip extraction . zip size: {len(zip_content)} bytes")
    found: Dict[str, str] = {}
    data = zip_content
    size = len(data)
    pos = 0
    

     # >>> ADDED SUMMARY COUNTERS
    total_entries = 0
    total_xml = 0
    total_xsd = 0
    total_nested = 0
    total_errors = 0
    #logs.append(f"Starting extraction. ZIP size: {size} bytes")
    #entry_count = 0

    cd_struct_fmt = '<IHHHHHHIIIHHHHHII'
    cd_struct_size = struct.calcsize(cd_struct_fmt)

    while True:
        pos = data.find(CD_SIG, pos)
        if pos == -1:
            break
        try:
            logger.debug(f"parsing cd entry at position {pos}")
            total_entries += 1  # >>> ADDED SUMMARY COUNTER

            if pos + cd_struct_size > size:
                logs.append(f"Central dir header truncated at {pos}.")
                total_errors += 1  # >>> ADDED SUMMARY COUNTER
                break

            header = struct.unpack_from(cd_struct_fmt, data, pos)
            (sig, ver_made, ver_needed, gp_flag, comp_method, mod_time, mod_date,
             crc32, comp_size, uncomp_size, fname_len, extra_len, comment_len,
             disk_start, int_attr, ext_attr, local_header_offset) = header

            fname_start = pos + cd_struct_size
            fname_end = fname_start + fname_len
            if fname_end > size:
                logs.append(f"CD filename truncated at {pos}.")
                total_errors += 1  # >>> ADDED SUMMARY COUNTER
                pos += 4
                continue

            raw_fname = data[fname_start:fname_end]
            try:
                fname = raw_fname.decode('utf-8')
            except Exception:
                fname = raw_fname.decode('latin1', errors='replace')

            norm_fname = fname.replace('\\', '/')
            base = os.path.basename(norm_fname)
            base_lower = base.lower()

            # Check if this is a target XML file
            is_target_xml = base_lower.endswith('.xml') and any(
                base_lower.startswith(p.lower()) for p in target_prefixes
            )
            
            # Check if this is a target XSD file (matching same prefixes)
            is_target_xsd = base_lower.endswith('.xsd') and any(
                base_lower.startswith(p.lower()) for p in target_prefixes
            )
            
            # Check for nested zip files
            is_nested_zip = base_lower.endswith('.zip')

            if is_target_xml or is_target_xsd or is_nested_zip:
                lh_off = local_header_offset

                if lh_off + 30 > size:
                    logs.append(f"Local header truncated at {lh_off} for {fname}; skipping.")
                    total_errors += 1  # >>> ADDED COUNTER
                    continue

                if data[lh_off:lh_off + 4] != LH_SIG:
                    search_from = max(0, lh_off - 8)
                    found_lh = data.find(LH_SIG, search_from, min(size, lh_off + 64))
                    if found_lh == -1:
                        logs.append(f"No local header found near {lh_off} for {fname}; skipping.")
                        total_errors += 1  # >>> ADDED
                        pos += 4
                        continue
                    else:
                        lh_off = found_lh

                lh_struct_fmt = '<IHHHHHIIIHH'
                lh = struct.unpack_from(lh_struct_fmt, data, lh_off)
                (_, ver_needed2, lh_flag, lh_comp_method, lh_time, lh_date,
                 lh_crc32, lh_comp_size, lh_uncomp_size, lh_fname_len, lh_extra_len) = lh

                data_start = lh_off + 30 + lh_fname_len + lh_extra_len
                read_comp_size = comp_size if comp_size != 0 else (lh_comp_size or 0)

                if data_start + read_comp_size > size:
                    logs.append(f"Compressed data window overruns buffer for {fname}. Trimming read size.")
                    logger.warning(f"compressed data window overruns buffer for {fname}")
                    total_errors += 1  # >>> ADDED
                    read_comp_size = max(0, min(read_comp_size, size - data_start))

                comp_bytes = data[data_start:data_start + read_comp_size]

                try:
                    if comp_method == 0:
                        file_bytes = comp_bytes
                    elif comp_method == 8:
                        file_bytes = zlib.decompress(comp_bytes, -zlib.MAX_WBITS)
                    else:
                        logs.append(f"Unsupported compression method {comp_method} for {fname}; skipping.")
                        logger.error(f"unsupported compression method {comp_method} for {fname}")
                        total_errors += 1  # >>> ADDED
                        pos += 4
                        continue
                except Exception as e:
                    logs.append(f"Decompression failed for {fname}: {e}")
                    logger.error(f"Decompression failed for {fname}: {e}")
                    total_errors += 1  # >>> ADDED
                    pos += 4
                    continue
                # --- FILE TYPE COUNTS ---
                if is_nested_zip:
                    total_nested += 1  # >>> ADDED 
                elif is_target_xsd:
                    total_xsd += 1  # >>> ADDED  COUNTER
                elif is_target_xml:
                    total_xml += 1  # >>> ADDED  COUNTER

                #entry_count += 1
                #if entry_count % 5 == 0:
                #   logger.debug(f"Processed {entry_count} files so far...")
                #   #logs.append(f"Processed {entry_count} files so far...")
                    
                if is_nested_zip:
                    logs.append(f"Found nested zip entry {norm_fname}, attempting in-memory extraction.")
                    logger.info(f"found nested zip entry {norm_fname}, attempting in-memory extraction.")
                    nested_found = extract_from_zip_bytes(file_bytes, logs, target_prefixes=target_prefixes)
                    # Add files from nested zip, ensuring keys are just basenames to avoid long paths
                    for k, v in nested_found.items():
                        if k not in found: # Avoid overwriting
                            found[k] = v
                elif is_target_xsd:
                    # Store XSD content with special prefix for easy matching
                    # Key format: __xsd__<basename_without_extension>
                    xsd_basename = os.path.splitext(base)[0].lower()
                    xsd_key = f'__xsd__{xsd_basename}'
                    found[xsd_key] = _decode_bytes_to_text(file_bytes)
                    logs.append(f"Extracted XSD: {norm_fname} -> {xsd_key}")
                    logger.info(f"extracted xsd: {norm_fname} -> {xsd_key}")   
                else:  # Regular XML file
                    text = _decode_bytes_to_text(file_bytes)
                    # Render HTML content if present
                    rendered_text = render_html_documentation(text)
                    key = base  # Use only the basename for the key
                    basekey = key
                    i = 1
                    while key in found:
                        key = f"{os.path.splitext(basekey)[0]}_{i}{os.path.splitext(basekey)[1]}"
                        i += 1
                    found[key] = rendered_text
                    logs.append(f"Extracted XML: {norm_fname} (size={len(rendered_text)} chars)")
                    logger.info(f"extracted xml: {norm_fname} (size={len(rendered_text)} chars)")
            
            pos += 4

        except Exception as e:
            logs.append(f"Error parsing CD entry at {pos}: {e}")
            logger.error(f"Error parsing CD entry at {pos}: {e}")
            pos += 4
    
     # >>> ADDED SUMMARY LOG
    logger.info(
        f"Summary: entries={total_entries}, xml={total_xml}, xsd={total_xsd}, nested={total_nested}, errors={total_errors}"
    )
    logs.append(
        f"Summary: entries={total_entries}, xml={total_xml}, xsd={total_xsd}, nested={total_nested}, errors={total_errors}"
    )

    xml_count = sum(1 for k in found if not k.startswith('__xsd__'))
    logs.append(f"Extraction complete: Found {xml_count} ACU configuration files.")
    logger.info(f"extraction complete: found {xml_count} acu configuration files")
    
    return found

def extract_from_directory(base_path: Union[str, Path], logs: List[str], target_prefixes: Tuple[str, ...]) -> Dict[str, str]:
    """
    FUNCTION: extract_from_directory

    DESCRIPTION:
        Recursively searches a directory for files with target prefixes and ZIP archives.
        Extracts relevant files and maintains relative paths in returned dictionary.
    
    USAGE:
        files = extract_from_directory("/tmp/data", logs, target_prefixes=('jdd', 'x3'))
    
    PARAMETERS:
        base_path (Union[str, Path]) : Directory path to search
        logs (List[str])              : List to append extraction log messages
        target_prefixes (Tuple[str, ...]) : File prefixes for XML/XSD extraction
    
    RETURNS:
        dict : Mapping of relative filenames to file content
    
    RAISES:
        None
    """
    base_path = Path(base_path)
    all_files = {}
    
    for item in base_path.rglob('*'):
        if not item.is_file():
            continue

        if item.suffix.lower() == '.zip':
            logs.append(f"Found ZIP archive: {item.relative_to(base_path)}")
            logger.info(f"found zip archive: {item.relative_to(base_path)}")
            try:
                with open(item, 'rb') as f:
                    zip_content = f.read()
                zip_files = extract_from_zip_bytes(zip_content, logs, target_prefixes)
                for fname, content in zip_files.items():
                    all_files[f"{item.name}/{fname}"] = content
            except Exception as e:
                logs.append(f"Error reading ZIP {item.name}: {e}")
                logger.error(f"error reading zip {item.name}: {e}")

        # Check for standalone XML files matching prefixes
        is_standalone_xml = item.suffix.lower() == '.xml' and \
                            any(item.name.lower().startswith(p.lower()) for p in target_prefixes)
        # Check for standalone XSD files matching prefixes
        is_standalone_xsd = item.suffix.lower() == '.xsd' and \
                            any(item.name.lower().startswith(p.lower()) for p in target_prefixes)

        if is_standalone_xml:
            logs.append(f"Found standalone XML: {item.relative_to(base_path)}")
            logger.info(f"found standalone xml: {item.relative_to(base_path)}")
            try:
                all_files[str(item.relative_to(base_path))] = item.read_text(encoding='utf-8', errors='ignore')
            except Exception as e:
                logs.append(f"Error reading file {item.name}: {e}")
                logger.error(f"error reading file {item.name}: {e}")
        elif is_standalone_xsd:
            logs.append(f"Found standalone XSD: {item.relative_to(base_path)}")
            logger.info(f"found standalone xsd: {item.relative_to(base_path)}")
            try:
                xsd_basename = os.path.splitext(item.name)[0].lower()
                xsd_key = f'__xsd__{xsd_basename}'
                all_files[xsd_key] = item.read_text(encoding='utf-8', errors='ignore')
            except Exception as e:
                logs.append(f"Error reading file {item.name}: {e}")
                logger.error(f"error reading file {item.name}: {e}")

    return all_files