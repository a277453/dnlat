import os
from modules.logging_config import logger
import logging

logger.info("Starting file_detector")

def detect_file_type_by_content(file_path: str) -> str:
    """
    FUNCTION: detect_file_type_by_content

    DESCRIPTION:
        Detects the type of a file by reading the first few lines (up to 400 bytes).
        Used for ambiguous file types such as `.jrn` and `.prn`.  
        The function checks for known keywords inside the content to determine
        whether it is a Customer Journal, UI Journal, TRC Error, or TRC Trace file.

    USAGE:
        result = detect_file_type_by_content("C:/logs/sample.jrn")

    PARAMETERS:
        file_path (str):
            Full path of the file to analyze. File should be readable in text mode.
        
    RETURNS:
        str :
            Possible return values:
            - "Customer Journal" : If content contains customer journal indicators
            - "UI Journal"       : If UI journal keywords are detected
            - "TRC Error"        : If error log signature is found
            - "TRC Trace"        : If trace log pattern is found
            - "Unknown"          : If no known pattern matches
            - "Unknown (access error)" : If file cannot be read

    RAISES:
        None :
            Errors are internally logged, and the function safely returns
            "Unknown (access error)" when an exception occurs.
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