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

logger = logging.getLogger(__name__)

# Low-level ZIP parsing constants
CD_SIG = b'PK\x01\x02'
LH_SIG = b'PK\x03\x04'

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
    
    def _extract_nested_zips(self, extract_path: Path, processed_zips: set) -> None:
        """
        Find and recursively extract any nested ZIP files using robust manual extraction.
        """
        nested_zips = [p for p in extract_path.rglob("*.zip") if p.is_file() and p not in processed_zips]
        if not nested_zips:
            return

        for zip_path in nested_zips:
            if zip_path in processed_zips:
                continue
            
            processed_zips.add(zip_path)
            logger.info(f"📦 Found nested ZIP: {zip_path.relative_to(self.base_extract_path)}. Extracting...")
            
            nested_extract_dir = zip_path.parent / zip_path.stem
            nested_extract_dir.mkdir(exist_ok=True)

            try:
                with zip_path.open('rb') as f:
                    zip_content = f.read()
                
                # Use the main extract_zip method recursively with robust extraction
                self.extract_zip(zip_content, is_nested=True, custom_path=str(nested_extract_dir))
                zip_path.unlink()
            except Exception as e:
                logger.error(f"  ❌ Error extracting nested ZIP {zip_path.name}: {e}. Leaving file as is.")

    def extract_zip(self, zip_content: bytes, is_nested: bool = False, custom_path: str = None) -> Path:
        """
        ROBUST extraction with manual member extraction to handle mixed path separators.
        Extracts all files, then filters relevant ones.

        Args:
            zip_content: The byte content of the ZIP file.
            is_nested: Flag to indicate if this is a nested ZIP extraction.
            custom_path: An optional custom path to extract to.
        """
        if not zip_content:
            raise ValueError("Empty ZIP file")
        
        if custom_path:
            extract_path = Path(custom_path)
            extract_path.mkdir(exist_ok=True, parents=True)
        else:
            extract_dir = tempfile.mkdtemp(
                prefix=f"dn_{int(time.time())}_", dir=self.base_extract_path
            )
            extract_path = Path(extract_dir)
        
        logger.info(f"Extracting to: {extract_path}")
        
        try:
            # Use BytesIO for speed
            with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zf:
                all_files = zf.namelist()
                logger.info(f"ZIP contains {len(all_files)} entries")

                # --- ROBUST MANUAL EXTRACTION to fix BadZipFile error ---
                extracted_count = 0
                for member in zf.infolist():
                    if member.is_dir():
                        continue

                    try:
                        # Normalize path to handle mixed separators (both / and \)
                        normalized_member_path = member.filename.replace('\\', '/')
                        target_path = extract_path / os.path.normpath(normalized_member_path)

                        # Security check: prevent path traversal attacks
                        if not str(target_path.resolve()).startswith(str(extract_path.resolve())):
                            logger.warning(f"⚠️  Skipping potentially unsafe path: {member.filename}")
                            continue
                        
                        # Create parent directories
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Manually extract file content
                        with zf.open(member) as source, open(target_path, "wb") as target:
                            shutil.copyfileobj(source, target)
                        
                        extracted_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️  Failed to extract {member.filename}: {e}")
                        continue
                
                logger.info(f"✓ Extracted {extracted_count} files. Now filtering for relevant files...")
                
                # Now walk the extracted directory and remove irrelevant files
                kept = 0
                removed = 0
                
                for root, dirs, files in os.walk(extract_path):
                    for file in files:
                        file_path = Path(root) / file
                        relative_path = file_path.relative_to(extract_path)
                        
                        if self.is_relevant_file(str(relative_path)):
                            kept += 1
                        else:
                            # Remove irrelevant file
                            try:
                                file_path.unlink()
                                removed += 1
                            except Exception as e:
                                logger.debug(f"Could not remove {file_path}: {e}")
                
                # Clean up empty directories
                for root, dirs, files in os.walk(extract_path, topdown=False):
                    for dir_name in dirs:
                        dir_path = Path(root) / dir_name
                        try:
                            if not any(dir_path.iterdir()):
                                dir_path.rmdir()
                        except Exception:
                            pass
                
                logger.info(f"✓ Kept {kept} relevant files, removed {removed} irrelevant files")
                
                if kept == 0:
                    raise ValueError("No relevant diagnostic files found in ZIP")

                # Only handle nested zips in the top-level call
                if not is_nested:
                    self._extract_nested_zips(extract_path, processed_zips=set())
                
                return extract_path
        
        except zipfile.BadZipFile as e:
            logger.error(f"❌ Invalid ZIP file: {e}")
            shutil.rmtree(extract_path, ignore_errors=True)
            raise ValueError(f"Invalid ZIP file: {str(e)}")
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
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
                        logger.debug(f"Cleaned up old extract: {extract_dir}")
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Cleanup error: {e}")

# --- ACU Parser Specific Extraction Logic ---

def _decode_bytes_to_text(b: bytes) -> str:
    """Decodes bytes to text, trying utf-8 then latin1."""
    try:
        return b.decode('utf-8')
    except Exception:
        return b.decode('latin1', errors='replace')

def render_html_documentation(html_string: str) -> str:
    """
    Renders simple HTML documentation into formatted text without external libraries.

    Args:
        html_string: The HTML content as a string.

    Returns:
        A formatted string with HTML tags replaced by newlines and markdown.
    """
    if not html_string or '<' not in html_string:
        return html_string

    # First, decode HTML entities like &lt; into <
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
    Low-level ZIP extractor that finds and decompresses target files from a byte stream.
    Extracts both XML files and their corresponding XSD files.
    Handles nested ZIPs by recursive extraction.
    Uses struct-based binary parsing to handle edge cases that zipfile module can't.
    
    Returns:
        Dictionary mapping filenames to their content. XSD files are prefixed with '__xsd__'
        followed by the base name (without extension) for easy matching.
    """
    found: Dict[str, str] = {}
    data = zip_content
    size = len(data)
    pos = 0
    
    logs.append(f"Starting extraction. ZIP size: {size} bytes")
    entry_count = 0

    cd_struct_fmt = '<IHHHHHHIIIHHHHHII'
    cd_struct_size = struct.calcsize(cd_struct_fmt)

    while True:
        pos = data.find(CD_SIG, pos)
        if pos == -1:
            break
        try:
            if pos + cd_struct_size > size:
                logs.append(f"Central dir header truncated at {pos}.")
                break

            header = struct.unpack_from(cd_struct_fmt, data, pos)
            (sig, ver_made, ver_needed, gp_flag, comp_method, mod_time, mod_date,
             crc32, comp_size, uncomp_size, fname_len, extra_len, comment_len,
             disk_start, int_attr, ext_attr, local_header_offset) = header

            fname_start = pos + cd_struct_size
            fname_end = fname_start + fname_len
            if fname_end > size:
                logs.append(f"CD filename truncated at {pos}.")
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
                    pos += 4
                    continue

                if data[lh_off:lh_off + 4] != LH_SIG:
                    search_from = max(0, lh_off - 8)
                    found_lh = data.find(LH_SIG, search_from, min(size, lh_off + 64))
                    if found_lh == -1:
                        logs.append(f"No local header found near {lh_off} for {fname}; skipping.")
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
                    read_comp_size = max(0, min(read_comp_size, size - data_start))

                comp_bytes = data[data_start:data_start + read_comp_size]

                try:
                    if comp_method == 0:
                        file_bytes = comp_bytes
                    elif comp_method == 8:
                        file_bytes = zlib.decompress(comp_bytes, -zlib.MAX_WBITS)
                    else:
                        logs.append(f"Unsupported compression method {comp_method} for {fname}; skipping.")
                        pos += 4
                        continue
                except Exception as e:
                    logs.append(f"Decompression failed for {fname}: {e}")
                    pos += 4
                    continue

                entry_count += 1
                if entry_count % 5 == 0:
                    logs.append(f"Processed {entry_count} files so far...")
                    
                if is_nested_zip:
                    logs.append(f"Found nested zip entry {norm_fname}, attempting in-memory extraction.")
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
            
            pos += 4

        except Exception as e:
            logs.append(f"Error parsing CD entry at {pos}: {e}")
            pos += 4

    xml_count = sum(1 for k in found if not k.startswith('__xsd__'))
    logs.append(f"Extraction complete: Found {xml_count} ACU configuration files.")
    
    return found

def extract_from_directory(base_path: Union[str, Path], logs: List[str], target_prefixes: Tuple[str, ...]) -> Dict[str, str]:
    """
    Recursively searches a directory for files starting with target_prefixes,
    and also extracts from any ZIP files found.
    """
    base_path = Path(base_path)
    all_files = {}
    
    for item in base_path.rglob('*'):
        if not item.is_file():
            continue

        if item.suffix.lower() == '.zip':
            logs.append(f"Found ZIP archive: {item.relative_to(base_path)}")
            try:
                with open(item, 'rb') as f:
                    zip_content = f.read()
                zip_files = extract_from_zip_bytes(zip_content, logs, target_prefixes)
                for fname, content in zip_files.items():
                    all_files[f"{item.name}/{fname}"] = content
            except Exception as e:
                logs.append(f"Error reading ZIP {item.name}: {e}")

        # Check for standalone XML files matching prefixes
        is_standalone_xml = item.suffix.lower() == '.xml' and \
                            any(item.name.lower().startswith(p.lower()) for p in target_prefixes)
        # Check for standalone XSD files matching prefixes
        is_standalone_xsd = item.suffix.lower() == '.xsd' and \
                            any(item.name.lower().startswith(p.lower()) for p in target_prefixes)

        if is_standalone_xml:
            logs.append(f"Found standalone XML: {item.relative_to(base_path)}")
            try:
                all_files[str(item.relative_to(base_path))] = item.read_text(encoding='utf-8', errors='ignore')
            except Exception as e:
                logs.append(f"Error reading file {item.name}: {e}")
        elif is_standalone_xsd:
            logs.append(f"Found standalone XSD: {item.relative_to(base_path)}")
            try:
                xsd_basename = os.path.splitext(item.name)[0].lower()
                xsd_key = f'__xsd__{xsd_basename}'
                all_files[xsd_key] = item.read_text(encoding='utf-8', errors='ignore')
            except Exception as e:
                logs.append(f"Error reading file {item.name}: {e}")

    return all_files