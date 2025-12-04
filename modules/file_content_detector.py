import os
from modules.logging_config import logger
import logging


logger.info("Starting file_detector")



def detect_file_type_by_content(file_path: str) -> str:
    """
    Detects the type of a file by reading its first few lines.
    This is used for ambiguous file types like .jrn and .prn.

    Args:
        file_path: The full path to the file.

    Returns:
        A string indicating the detected file type or "Unknown".
    """
    logger.debug(f"Detecting file type by content: {file_path}")
    try:
        # Read the first 400 bytes, which is enough to find the identifier.
        with open(file_path, 'r', errors='ignore') as f:
            content = f.read(400)

        content_lower = content.lower()

        if "customer journal" in content_lower:
            logger.debug(f"Detected as Customer Journal: {file_path}")
            return "Customer Journal"
        if "ui journal" in content_lower:
            logger.debug(f"Detected as UI Journal: {file_path}")
            return "UI Journal"
        if "trc_error" in content_lower:
            logger.debug(f"Detected as TRC Error: {file_path}")
            return "TRC Error"
        if "trc_trace" in content_lower:
            logger.debug(f"Detected as TRC Trace: {file_path}")
            return "TRC Trace"
            
    except (IOError, OSError)  as e:
        logger.error(f"Error reading file '{file_path}': {e}")
        # Handle cases where the file can't be read
        return "Unknown (access error)"

    return "Unknown"