import unittest
import zipfile
import io
import sys
from pathlib import Path
import tempfile

# Add the project root to the Python path to allow imports from modules
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from modules.extraction import extract_from_zip_bytes, _decode_bytes_to_text, render_html_documentation, extract_from_directory

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

    def test_multiple_matching_files(self):
        """Tests that multiple files matching the prefix are extracted."""
        zip_content = create_zip_in_memory({
            "jdd_1.xml": "<data>1</data>",
            "jdd_2.xml": "<data>2</data>",
            "other.xml": "<data>other</data>"
        })
        logs = []
        result = extract_from_zip_bytes(zip_content, logs, target_prefixes=('jdd',))
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result["jdd_1.xml"], "<data>1</data>")
        self.assertEqual(result["jdd_2.xml"], "<data>2</data>")

    def test_duplicate_filenames_in_different_folders(self):
        """Tests handling of duplicate filenames in different directories."""
        zip_content = create_zip_in_memory({
            "folder1/jdd_dup.xml": "content1",
            "folder2/jdd_dup.xml": "content2"
        })
        logs = []
        result = extract_from_zip_bytes(zip_content, logs, target_prefixes=('jdd',))
        
        # The parser should rename duplicates to avoid overwriting.
        # We expect 'jdd_dup.xml' and 'jdd_dup_1.xml'
        self.assertEqual(len(result), 2)
        self.assertIn("jdd_dup.xml", result)
        self.assertIn("jdd_dup_1.xml", result)
        
        # Verify content is preserved
        values = list(result.values())
        self.assertIn("content1", values)
        self.assertIn("content2", values)

    def test_decode_bytes_fallback(self):
        """Tests that _decode_bytes_to_text falls back to latin1 if utf-8 fails."""
        # Valid UTF-8
        self.assertEqual(_decode_bytes_to_text(b'hello'), 'hello')
        # Invalid UTF-8 sequence (0xE9 is 'é' in Latin-1, but invalid start byte in UTF-8)
        self.assertEqual(_decode_bytes_to_text(b'\xe9'), 'é')

    def test_render_html_documentation(self):
        """Tests that HTML documentation is rendered to plain text correctly."""
        html_text = "<p>Para 1</p><br><b>Bold</b> and <i>Italic</i>"
        result = render_html_documentation(html_text)
        # render_html_documentation replaces <p> with empty, </p> with \n\n, <br> with \n
        # <b> -> **, <i> -> *
        self.assertIn("Para 1", result)
        self.assertIn("**Bold**", result)
        self.assertIn("*Italic*", result)

    def test_zip_stored_compression(self):
        """Tests extraction of ZIP entries with STORED (no compression) method."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_STORED) as zf:
            zf.writestr("jdd_stored.xml", "<data>stored</data>")
        zip_content = zip_buffer.getvalue()
        
        logs = []
        result = extract_from_zip_bytes(zip_content, logs, target_prefixes=('jdd',))
        
        self.assertIn("jdd_stored.xml", result)
        self.assertEqual(result["jdd_stored.xml"], "<data>stored</data>")

    def test_extract_from_directory(self):
        """Tests extraction from a directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            
            # 1. Create a standalone XML file
            xml_file = base_path / "jdd_standalone.xml"
            xml_file.write_text("<data>standalone</data>", encoding='utf-8')
            
            # 2. Create a standalone XSD file
            xsd_file = base_path / "x3_schema.xsd"
            xsd_file.write_text("<schema>x3</schema>", encoding='utf-8')
            
            # 3. Create a ZIP file in a subdirectory
            sub_dir = base_path / "subdir"
            sub_dir.mkdir()
            zip_content = create_zip_in_memory({"jdd_inside.xml": "<data>inside</data>"})
            zip_file = sub_dir / "data.zip"
            zip_file.write_bytes(zip_content)
            
            logs = []
            results = extract_from_directory(base_path, logs, target_prefixes=('jdd', 'x3'))
            
            # Verify standalone XML (extract_from_directory uses relative paths as keys)
            self.assertIn("jdd_standalone.xml", results)
            self.assertIn("__xsd__x3_schema", results)
            
            # Verify ZIP content (key format is "zipname/filename")
            zip_key = "data.zip/jdd_inside.xml"
            self.assertIn(zip_key, results)
            self.assertEqual(results[zip_key], "<data>inside</data>")

    def test_files_skipped_by_prefix(self):
        """Tests that files not matching target prefixes are ignored."""
        zip_content = create_zip_in_memory({
            "jdd_match.xml": "<data>match</data>",
            "ignore_me.xml": "<data>ignore</data>",
            "x3_match.xsd": "<schema>match</schema>"
        })
        logs = []
        result = extract_from_zip_bytes(zip_content, logs, target_prefixes=('jdd', 'x3'))
        
        self.assertIn("jdd_match.xml", result)
        self.assertIn("__xsd__x3_match", result)
        self.assertNotIn("ignore_me.xml", result)

    def test_unsupported_compression_method(self):
        """Tests that entries with unsupported compression methods are skipped."""
        # Create a valid zip with stored (method 0)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_STORED) as zf:
            zf.writestr("jdd_bad_comp.xml", "content")
        
        data = bytearray(zip_buffer.getvalue())
        # Locate Central Directory Signature PK\x01\x02 and change compression method (offset 10) to 99
        cd_pos = data.find(b'PK\x01\x02')
        if cd_pos != -1:
            data[cd_pos + 10] = 99 
            logs = []
            result = extract_from_zip_bytes(bytes(data), logs, target_prefixes=('jdd',))
            self.assertNotIn("jdd_bad_comp.xml", result)

if __name__ == '__main__':
    unittest.main()