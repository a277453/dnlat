import unittest
import tempfile
import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from modules.registry_analyzer import RegistryAnalyzerService

class TestRegistryParser(unittest.TestCase):

    def setUp(self):
        """Set up the test environment."""
        self.service = RegistryAnalyzerService()
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        """Clean up the test environment."""
        self.temp_dir.cleanup()

    def _create_reg_file(self, filename, content):
        """Helper to create a temporary .reg file."""
        path = os.path.join(self.temp_dir.name, filename)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return path

    def test_parse_simple_registry_file(self):
        """Tests parsing of a basic registry file."""
        content = """
[HKEY_LOCAL_MACHINE\\SOFTWARE\\Test]
"Key1"="Value1"
"Key2"=dword:00000001
"""
        file_path = self._create_reg_file("test1.reg", content)
        result = self.service.view_registry_file(file_path)
        
        self.assertTrue(result["parsed"])
        self.assertEqual(result["count"], 2)
        
        entries = result["entries"]
        self.assertEqual(entries[0]["Key"], "Key1")
        self.assertEqual(entries[0]["Value"], '"Value1"')
        self.assertEqual(entries[1]["Key"], "Key2")
        self.assertEqual(entries[1]["Value"], 'dword:00000001')

    def test_compare_registry_files(self):
        """Tests the comparison logic for added, removed, and changed keys."""
        content_a = """
[HKEY_LOCAL_MACHINE\\SOFTWARE\\Test]
"CommonKey"="OriginalValue"
"RemovedKey"="ShouldBeRemoved"
"""
        content_b = """
[HKEY_LOCAL_MACHINE\\SOFTWARE\\Test]
"CommonKey"="ChangedValue"
"AddedKey"="ShouldBeAdded"
"""
        file_a = self._create_reg_file("a.reg", content_a)
        file_b = self._create_reg_file("b.reg", content_b)
        
        diff = self.service.compare_registry_files(file_a, file_b)
        
        self.assertEqual(len(diff["changed"]), 1)
        self.assertEqual(diff["changed"][0]["Key"], "CommonKey")
        self.assertEqual(diff["changed"][0]["Value_A"], '"OriginalValue"')
        self.assertEqual(diff["changed"][0]["Value_B"], '"ChangedValue"')
        
        self.assertEqual(len(diff["added"]), 1)
        self.assertEqual(diff["added"][0]["Key"], "AddedKey")
        
        self.assertEqual(len(diff["removed"]), 1)
        self.assertEqual(diff["removed"][0]["Key"], "RemovedKey")

if __name__ == '__main__':
    unittest.main()