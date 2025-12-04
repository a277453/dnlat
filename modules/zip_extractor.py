import struct
import zlib
import os
from typing import Dict, List
from modules.logging_config import logger
import logging


logger.info("zip_extractor initialized")



CD_SIG = b'PK\x01\x02'
LH_SIG = b'PK\x03\x04'


def _decode_bytes_to_text(b: bytes) -> str:
	"""Decodes bytes to text, trying utf-8 then latin1."""
	try:
		text = b.decode('utf-8')
		logger.debug("Decoded bytes using utf-8")
		return text
	except Exception:
		text = b.decode('latin1', errors='replace')
		logger.debug("Decoded bytes using latin1 with replacement")
		return text
	

def extract_from_zip_bytes(zip_bytes: bytes, logs: List[str], target_prefixes=('jdd', 'x3')) -> Dict[str, str]:
	"""
	Low-level ZIP extractor that finds and decompresses target files from a byte stream.
	Extracts both XML files and their corresponding XSD files.
	Handles nested ZIPs by recursive extraction.
	
	Returns:
		Dictionary mapping filenames to their content. XSD files are prefixed with '__xsd__'
		followed by the base name (without extension) for easy matching.
	"""
	found: Dict[str, str] = {}
	data = zip_bytes
	size = len(data)
	pos = 0
	
	logs.append(f"Starting extraction. ZIP size: {size} bytes")
	logger.info(f"Starting extraction. ZIP size: {size} bytes")

	entry_count = 0

	cd_struct_fmt = '<IHHHHHHIIIHHHHHII'
	cd_struct_size = struct.calcsize(cd_struct_fmt)

	while True:
		pos = data.find(CD_SIG, pos)
		if pos == -1:
			break
		try:
			logger.debug(f"reading Central Dir entry at position {pos}")
			if pos + cd_struct_size > size:
				logs.append(f"Central dir header truncated at {pos}.")
				logger.warning(f"Central dir header truncated at {pos}.")
				break

			header = struct.unpack_from(cd_struct_fmt, data, pos)
			(sig, ver_made, ver_needed, gp_flag, comp_method, mod_time, mod_date,
			 crc32, comp_size, uncomp_size, fname_len, extra_len, comment_len,
			 disk_start, int_attr, ext_attr, local_header_offset) = header

			fname_start = pos + cd_struct_size
			fname_end = fname_start + fname_len

			if fname_end > size:
				logs.append(f"CD filename truncated at {pos}.")
				logger.warning(f"CD filename truncated at {pos}.")
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
					logger.warning(f"Local header truncated at {lh_off} for {fname}; skipping.")	
					pos += 4
					continue

				if data[lh_off:lh_off + 4] != LH_SIG:
					search_from = max(0, lh_off - 8)
					found_lh = data.find(LH_SIG, search_from, min(size, lh_off + 64))

					if found_lh == -1:
						logs.append(f"No local header found near {lh_off} for {fname}; skipping.")
						logger.warning(f"No local header found near {lh_off} for {fname}; skipping.")
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
						logger.error(f"Unsupported compression method {comp_method} for {fname}; skipping.")

						pos += 4
						continue
				except Exception as e:
					logs.append(f"Decompression failed for {fname}: {e}")
					logger.error(f"Decompression failed for {fname}: {e}")
					pos += 4
					continue
				

				entry_count += 1
				if entry_count % 5 == 0:
					logger.debug(f"Processed {entry_count} files so far...")
					
				if is_nested_zip:
					logger.info(f"Found nested zip entry {norm_fname}, attempting in-memory extraction.")

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

					logger.info(f"Extracted XSD: {norm_fname} -> {xsd_key}")          

				else:  # Regular XML file
					text = _decode_bytes_to_text(file_bytes)
					key = base # Use only the basename for the key
					basekey = key
					i = 1
					while key in found:
						key = f"{os.path.splitext(basekey)[0]}_{i}{os.path.splitext(basekey)[1]}"
						i += 1
					found[key] = text
					logger.info(f"Extracted XML: {norm_fname} (size={len(text)} chars)")


			pos = fname_end + extra_len + comment_len

		except Exception as e:
			logs.append(f"Error parsing CD entry at {pos}: {e}")
			logger.error(f"Error parsing CD entry at {pos}: {e}")
			pos += 4

	xml_count = sum(1 for k in found if not k.startswith('__xsd__'))
	xsd_count = sum(1 for k in found if k.startswith('__xsd__'))
	logger.info(f"Extraction complete: {xml_count} XML files, {xsd_count} XSD files")

	
	return found
