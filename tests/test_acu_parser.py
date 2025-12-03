import unittest
import zipfile
import io
import sys
from pathlib import Path

# Add the project root to the Python path to allow imports from modules
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from modules.extraction import extract_from_zip_bytes

def create_zip_in_memory(file_dict: dict) -> bytes:
    """Helper function to create a ZIP archive in memory."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename, content in file_dict.items():
            if isinstance(content, bytes):
                zf.writestr(filename, content)
            else:
                zf.writestr(filename, content.encode('utf-8'))
    zip_buffer.seek(0)
    return zip_buffer.read()

class TestACUParser(unittest.TestCase):

    def test_extract_jdd_xml_file(self):
        """Tests that a standard jdd XML file is extracted correctly."""
        zip_content = create_zip_in_memory({
            "jddtest.xml": "<data>jdd</data>",
            "other.xml": "<data>other</data>"
        })
        logs = []
        result = extract_from_zip_bytes(zip_content, logs, target_prefixes=('jdd',))
        
        self.assertIn("jddtest.xml", result)
        self.assertEqual(result["jddtest.xml"], "<data>jdd</data>")
        self.assertNotIn("other.xml", result)

    def test_extract_x3_xsd_file(self):
        """Tests that an XSD file is extracted with the correct '__xsd__' key format."""
        zip_content = create_zip_in_memory({
            "x3config.xsd": "<schema>x3</schema>"
        })
        logs = []
        result = extract_from_zip_bytes(zip_content, logs, target_prefixes=('x3',))
        
        self.assertIn("__xsd__x3config", result)
        self.assertEqual(result["__xsd__x3config"], "<schema>x3</schema>")

    def test_nested_zip_extraction(self):
        """Tests that the parser can extract target files from a nested ZIP."""
        nested_zip_content = create_zip_in_memory({
            "inner_folder/jdd_nested.xml": "<data>nested</data>"
        })
        
        outer_zip_content = create_zip_in_memory({
            "archive.zip": nested_zip_content,
            "toplevel.txt": "ignore"
        })
        
        logs = []
        result = extract_from_zip_bytes(outer_zip_content, logs, target_prefixes=('jdd',))
        
        self.assertIn("jdd_nested.xml", result)
        self.assertEqual(result["jdd_nested.xml"], "<data>nested</data>")

    def test_mixed_path_separators(self):
        """Tests handling of Windows-style path separators."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Manually create an entry with a Windows path
            info = zipfile.ZipInfo("folder\\jdd_win.xml")
            zf.writestr(info, "<data>windows</data>")
        zip_content = zip_buffer.getvalue()

        logs = []
        result = extract_from_zip_bytes(zip_content, logs, target_prefixes=('jdd',))
        
        self.assertIn("jdd_win.xml", result)
        self.assertEqual(result["jdd_win.xml"], "<data>windows</data>")

    def test_empty_or_corrupt_zip(self):
        """Tests that the parser handles empty or invalid data gracefully."""
        logs = []
        result = extract_from_zip_bytes(b"invalid zip data", logs)
        self.assertEqual(result, {})

if __name__ == '__main__':
    unittest.main()