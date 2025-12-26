"""
FAST ZIP Extraction - Optimized for Speed
Extracts relevant DN diagnostic files while preserving folder structure for proper categorization.
"""
from enum import member
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
    """

    def __init__(self):
        """
        FUNCTION: __init__

        DESCRIPTION:
            Initialize ZipExtractionService, setting base extraction path, relevant file patterns, and skip patterns.
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
            '.jrn',
            '.prn',
            'jdd',
            'x3',
            '.zip'
        }

        self.skip_patterns = {
            '__macosx',
            '.ds_store',
            'thumbs.db',
            'desktop.ini',
            '.git',
            '.svn'
        }

        logger.info(f"ZipExtractionService initialized at {self.base_extract_path}")

    def is_relevant_file(self, filename: str) -> bool:
        """
        FUNCTION: is_relevant_file

        DESCRIPTION:
            Checks if a given file is relevant for DN diagnostics based on predefined patterns and skip rules.
        """
        filename_lower = filename.lower()
        basename = os.path.basename(filename_lower)

        for skip in self.skip_patterns:
            if skip in filename_lower:
                return False

        if basename.startswith('.'):
            return False

        for pattern in self.relevant_patterns:
            if pattern in filename_lower:
                logger.debug(f"File is relevant: {filename}")
                return True
        
        return False

    def _extract_nested_zips(self, extract_path: Path):
        """
        FUNCTION: _extract_nested_zips

        DESCRIPTION:
            Extracts ONLY first-level nested ZIP files found in the extraction directory.
            Does NOT recurse deeper than one level.
            Does NOT remove nested ZIP files after extraction.
        
        FIXED: 
            - Only processes first-level nested ZIPs (no deeper recursion)
            - Preserves nested ZIP files after extraction
            - Handles Windows-style backslash paths in ZIP files
        """
        # Find nested ZIPs at FIRST LEVEL ONLY (not recursive)
        # Using glob() instead of rglob() to avoid finding deeply nested ZIPs
        nested_zips = [
            p for p in extract_path.glob("**/*.zip")  # First level subdirectories only
            if p.is_file() and p.parent != extract_path  # Skip ZIPs in root
        ]
        
        # Also check root directory for ZIPs
        root_zips = [
            p for p in extract_path.glob("*.zip")
            if p.is_file()
        ]
        
        # Combine both lists
        nested_zips.extend(root_zips)
        
        nested_zip_count = len(nested_zips)

        if nested_zip_count == 0:
            logger.info("No first-level nested ZIP files found")
            return

        zip_names = [str(p.relative_to(extract_path)) for p in nested_zips]
        logger.info(
            f"Found {nested_zip_count} first-level nested ZIP file(s): {', '.join(zip_names)}"
        )

        # Process each nested ZIP ONCE (no recursion)
        for index, zip_path in enumerate(nested_zips, start=1):
            logger.info(
                f"Extracting nested ZIP ({index}/{nested_zip_count}): "
                f"{zip_path.relative_to(extract_path)}"
            )

            # Extract to subdirectory with same name as ZIP (preserves hierarchy)
            nested_extract_dir = zip_path.parent / zip_path.stem
            nested_extract_dir.mkdir(exist_ok=True)
            logger.debug(f"zip_path.parent directory: {zip_path.parent}")
            logger.debug(f"zip_path.stem directory: {zip_path.stem}")
            logger.debug(f"Nested extract directory: {nested_extract_dir}")

            try:
                logger.debug(f"Opening nested ZIP: {zip_path}")
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    extracted_count = 0
                    failed_count = 0
                    
                    all_members = zf.namelist()
                    logger.debug(f"Nested ZIP contains {len(all_members)} total entries")
                    
                    for member in all_members:
                        # Check if file is relevant
                        if not self.is_relevant_file(member):
                            logger.debug(f"Skipping irrelevant file: {member}")
                            continue
                        
                        try:
                            logger.debug(f"NKS Extracting: {member}")
                            #normalized_member = zip_path / member
                            
                            # Read file data from ZIP
                            #file_data = zf.read(normalized_member)
                            #file_data = zf.read(member)
                            file_data = zf.read(member)
                            # if member.count('CUSTOMER/20250311.jrn') > 0:
                            #     logger.debug("Found test file CUSTOMER/20250311.jrn in nested ZIP")
                            #     member = member.replace('/', '\\')
                            #     logger.debug(f"NKS Extracting 2: {member}")
                            #     file_data = zf.read(member)
                            # else:
                            #     member = member.replace('/', '\\')
                            #     logger.debug(f"NKS Extracting 3: {member}")
                            #     file_data = zf.read(member)
                            #     continue
                            
                            #     file_data = f.read()

                            
                            
                            # Normalize path (convert backslashes to forward slashes)
                            #normalized_member = member.replace('\\', '/')
                            #normalized_member = member
                            logger.debug(f">>sp Extracting: {member}")
                            
                            # Create target path
                            #target_path = nested_extract_dir / normalized_member
                            target_path = nested_extract_dir / member
                            
                            # Create parent directories
                            target_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            # Write file
                            with open(target_path, 'wb') as f:
                                f.write(file_data)
                            
                            extracted_count += 1
                            logger.debug(f"  ✓ Successfully extracted: {member}")
                            
                        except KeyError as e:
                            logger.error(f"  ✗ File not found in ZIP: {member}")
                            failed_count += 1
                            continue
                            
                        except Exception as e:
                            logger.error(f"  ✗ Failed to extract {member}: {e}", exc_info=True)
                            failed_count += 1
                            continue
                    
                    logger.info(
                        f"Nested ZIP extraction complete: "
                        f"{extracted_count} files extracted, "
                        f"{failed_count} files failed"
                    )
                    
                    if extracted_count == 0 and failed_count > 0:
                        logger.error(
                            f"Failed to extract any files from nested ZIP: {zip_path.name}. "
                            f"This may indicate a corrupted or incompatible ZIP file."
                        )

                # IMPORTANT: Do NOT delete nested ZIP file
                # User wants to keep nested ZIPs after extraction
                logger.info(f"✓ Keeping nested ZIP file: {zip_path.name}")
                
            except zipfile.BadZipFile as e:
                logger.error(f"BadZipFile error for nested ZIP {zip_path.name}: {e}")
                continue
            except Exception as e:
                logger.error(
                    f"Error extracting nested ZIP {zip_path.name}: {e}",
                    exc_info=True
                )
                continue
        
        logger.info(
            f"First-level nested ZIP extraction complete. "
            f"Processed {nested_zip_count} ZIP file(s). "
            f"No deeper recursion performed."
        )

    def extract_zip(self, zip_content: bytes) -> Path:
        """
        FUNCTION: extract_zip

        DESCRIPTION:
            Extracts all relevant files from a ZIP archive into a temporary directory.
            Handles ONLY first-level nested ZIPs (no deeper recursion).
            Keeps nested ZIP files after extraction.
        """

        if not zip_content:
            raise ValueError("Empty ZIP file")

        extract_dir = tempfile.mkdtemp(
            prefix=f"dn_{int(time.time())}_",
            dir=self.base_extract_path
        )
        extract_path = Path(extract_dir)

        logger.info(f"Extracting to: {extract_path}")

        try:
            with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zf:
                all_files = zf.namelist()
                logger.info(f"ZIP contains {len(all_files)} entries")
                
                # Filter ONLY relevant files
                relevant_files = [
                    f for f in all_files 
                    if not f.endswith('/') and self.is_relevant_file(f)
                ]
                
                logger.info(
                    f"Extracting {len(relevant_files)} relevant files "
                    f"(skipping {len(all_files) - len(relevant_files)} irrelevant)"
                )
                
                if not relevant_files:
                    raise ValueError("No relevant diagnostic files found in ZIP")
                
                # Extract only relevant files
                extracted = 0
                for filename in relevant_files:
                    try:
                        logger.info(f"NKS 1: {filename}")
                        logger.info(f"NKS 2: {extract_path}")
                        # #filename = filename.replace('\\', '/')
                        # #if filename.count('CUSTOMER/20250311.jrn') > 0:
                        # if filename.count('.jrn') > 0:
                                
                        #         filename = filename.replace('/', '\\')
                        #         logger.info(f"NKS 3: {filename}")
                        #         continue
                        
                        # finalppath = extract_path / filename
                        
                        # if finalppath.exists():
                        #     logger.info(f"NKS 3: {finalppath}")
                        # else:
                        #     zf.extract(filename, extract_path)
                        zf.extract(filename, extract_path)
                        extracted += 1
                    except Exception as e:
                        logger.warning(f"NKS Skip {filename}: {e}")
                        continue
                
                logger.info(f"Extracted {extracted} files successfully")
                
                if extracted == 0:
                    raise ValueError("Failed to extract any files")
                
                # Extract ONLY first-level nested ZIPs (no recursion)
                logger.info("Scanning for first-level nested ZIP files...")
                self._extract_nested_zips(extract_path)
                
                # Log final file count
                all_files_after = [p for p in extract_path.rglob('*') if p.is_file()]
                logger.info(
                    f"Total files after first-level nested ZIP extraction: "
                    f"{len(all_files_after)}"
                )
                
                return extract_path

        except zipfile.BadZipFile:
            shutil.rmtree(extract_path, ignore_errors=True)
            raise ValueError("Invalid ZIP file")
        except Exception as e:
            shutil.rmtree(extract_path, ignore_errors=True)
            raise Exception(f"Extraction failed: {str(e)}")
    def cleanup_old_extracts(self, max_age_hours: int = 24):
        """
        FUNCTION: cleanup_old_extracts

        DESCRIPTION:
            Deletes old extraction directories older than the specified age to save disk space.
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
# --- ACU Parser Specific Extraction Logic (UNCHANGED) ---

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
    logger.info(f"Starting low-level ACU ZIP extraction. ZIP size: {len(zip_content)} bytes")
    found: Dict[str, str] = {}
    data = zip_content
    size = len(data)
    pos = 0

    total_entries = 0
    total_xml = 0
    total_xsd = 0
    total_nested = 0
    total_errors = 0

    cd_struct_fmt = '<IHHHHHHIIIHHHHHII'
    cd_struct_size = struct.calcsize(cd_struct_fmt)

    while True:
        pos = data.find(CD_SIG, pos)
        if pos == -1:
            break
        try:
            logger.debug(f"Parsing CD entry at position {pos}")
            total_entries += 1

            if pos + cd_struct_size > size:
                logs.append(f"Central dir header truncated at {pos}.")
                total_errors += 1
                break

            header = struct.unpack_from(cd_struct_fmt, data, pos)
            (sig, ver_made, ver_needed, gp_flag, comp_method, mod_time, mod_date,
             crc32, comp_size, uncomp_size, fname_len, extra_len, comment_len,
             disk_start, int_attr, ext_attr, local_header_offset) = header

            fname_start = pos + cd_struct_size
            fname_end = fname_start + fname_len
            if fname_end > size:
                logs.append(f"CD filename truncated at {pos}.")
                total_errors += 1
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
                    total_errors += 1
                    pos += 4
                    continue

                if data[lh_off:lh_off + 4] != LH_SIG:
                    search_from = max(0, lh_off - 8)
                    found_lh = data.find(LH_SIG, search_from, min(size, lh_off + 64))
                    if found_lh == -1:
                        logs.append(f"No local header found near {lh_off} for {fname}; skipping.")
                        total_errors += 1
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
                    logger.warning(f"Compressed data window overruns buffer for {fname}. Trimming read size.")
                    read_comp_size = max(0, min(read_comp_size, size - data_start))

                comp_bytes = data[data_start:data_start + read_comp_size]

                try:
                    if comp_method == 0:
                        file_bytes = comp_bytes
                    elif comp_method == 8:
                        file_bytes = zlib.decompress(comp_bytes, -zlib.MAX_WBITS)
                    else:
                        logs.append(f"Unsupported compression method {comp_method} for {fname}; skipping.")
                        logger.error(f"Unsupported compression method {comp_method} for {fname}")
                        total_errors += 1
                        pos += 4
                        continue
                except Exception as e:
                    logs.append(f"Decompression failed for {fname}: {e}")
                    logger.error(f"Decompression failed for {fname}: {e}")
                    total_errors += 1
                    pos += 4
                    continue
                
                # File type counts
                if is_nested_zip:
                    total_nested += 1
                elif is_target_xsd:
                    total_xsd += 1
                elif is_target_xml:
                    total_xml += 1
                    
                if is_nested_zip:
                    logs.append(f"Found nested zip entry {norm_fname}, attempting in-memory extraction.")
                    logger.info(f"Found nested zip entry {norm_fname}, attempting in-memory extraction.")
                    nested_found = extract_from_zip_bytes(file_bytes, logs, target_prefixes=target_prefixes)
                    for k, v in nested_found.items():
                        if k not in found:
                            found[k] = v
                elif is_target_xsd:
                    xsd_basename = os.path.splitext(base)[0].lower()
                    xsd_key = f'__xsd__{xsd_basename}'
                    found[xsd_key] = _decode_bytes_to_text(file_bytes)
                    logs.append(f"Extracted XSD: {norm_fname} -> {xsd_key}")
                    logger.info(f"Extracted XSD: {norm_fname} -> {xsd_key}")   
                else:  # Regular XML file
                    text = _decode_bytes_to_text(file_bytes)
                    rendered_text = render_html_documentation(text)
                    key = base
                    basekey = key
                    i = 1
                    while key in found:
                        key = f"{os.path.splitext(basekey)[0]}_{i}{os.path.splitext(basekey)[1]}"
                        i += 1
                    found[key] = rendered_text
                    logs.append(f"Extracted XML: {norm_fname} (size={len(rendered_text)} chars)")
                    logger.info(f"Extracted XML: {norm_fname} (size={len(rendered_text)} chars)")
            
            pos += 4

        except Exception as e:
            logs.append(f"Error parsing CD entry at {pos}: {e}")
            logger.error(f"Error parsing CD entry at {pos}: {e}")
            pos += 4
    
    logger.info(
        f"ACU extraction summary: entries={total_entries}, xml={total_xml}, xsd={total_xsd}, nested={total_nested}, errors={total_errors}"
    )
    logs.append(
        f"Summary: entries={total_entries}, xml={total_xml}, xsd={total_xsd}, nested={total_nested}, errors={total_errors}"
    )

    xml_count = sum(1 for k in found if not k.startswith('__xsd__'))
    logs.append(f"Extraction complete: Found {xml_count} ACU configuration files.")
    logger.info(f"ACU extraction complete: found {xml_count} configuration files")
    
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
            logger.info(f"Found ZIP archive: {item.relative_to(base_path)}")
            try:
                with open(item, 'rb') as f:
                    zip_content = f.read()
                zip_files = extract_from_zip_bytes(zip_content, logs, target_prefixes)
                for fname, content in zip_files.items():
                    all_files[f"{item.name}/{fname}"] = content
            except Exception as e:
                logs.append(f"Error reading ZIP {item.name}: {e}")
                logger.error(f"Error reading ZIP {item.name}: {e}")

        # Check for standalone XML files matching prefixes
        is_standalone_xml = item.suffix.lower() == '.xml' and \
                            any(item.name.lower().startswith(p.lower()) for p in target_prefixes)
        # Check for standalone XSD files matching prefixes
        is_standalone_xsd = item.suffix.lower() == '.xsd' and \
                            any(item.name.lower().startswith(p.lower()) for p in target_prefixes)

        if is_standalone_xml:
            logs.append(f"Found standalone XML: {item.relative_to(base_path)}")
            logger.info(f"Found standalone XML: {item.relative_to(base_path)}")
            try:
                all_files[str(item.relative_to(base_path))] = item.read_text(encoding='utf-8', errors='ignore')
            except Exception as e:
                logs.append(f"Error reading file {item.name}: {e}")
                logger.error(f"Error reading file {item.name}: {e}")
        elif is_standalone_xsd:
            logs.append(f"Found standalone XSD: {item.relative_to(base_path)}")
            logger.info(f"Found standalone XSD: {item.relative_to(base_path)}")
            try:
                xsd_basename = os.path.splitext(item.name)[0].lower()
                xsd_key = f'__xsd__{xsd_basename}'
                all_files[xsd_key] = item.read_text(encoding='utf-8', errors='ignore')
            except Exception as e:
                logs.append(f"Error reading file {item.name}: {e}")
                logger.error(f"Error reading file {item.name}: {e}")

    return all_files