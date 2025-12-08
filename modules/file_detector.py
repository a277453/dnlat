from .configManager import detect_file_type as original_detect_file_type

def detect_file_type(file_path: str) -> str:
    """
    Wrapper for the original detect_file_type function.
    Allows for future modifications without changing the original.
    """
    return original_detect_file_type(file_path)