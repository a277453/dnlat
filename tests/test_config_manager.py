import unittest
import sys
import os
import tempfile
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from modules.configManager import xml_to_dict, validate_xml_config, detect_file_type

class TestConfigManager(unittest.TestCase):

    def setUp(self):
        """Create a temporary directory and a valid config file for testing."""
        self.test_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.test_dir.name, "test_config.xml")
        
        self.valid_xml = """
        <configuration>
            <transactionList>
                <transaction>
                    <key>T1</key>
                    <value>Type1</value>
                </transaction>
            </transactionList>
            <customerJournalParsing>
                <starttransaction>S1, S2</starttransaction>
                <endtransaction>E1, E2</endtransaction>
                <chainingtransaction>C1</chainingtransaction>
            </customerJournalParsing>
        </configuration>
        """
        with open(self.config_path, 'w', encoding='utf-8') as f:
            f.write(self.valid_xml)

    def tearDown(self):
        """Cleanup temporary directory."""
        self.test_dir.cleanup()

    def test_xml_to_dict_parsing(self):
        """Test that xml_to_dict correctly extracts transaction mappings and TIDs."""
        real_name, start, end, chain = xml_to_dict(self.config_path)
        
        self.assertEqual(real_name.get('T1'), 'Type1')
        self.assertIn('S1', start)
        self.assertIn('S2', start)
        self.assertIn('E1', end)
        self.assertIn('C1', chain)

    def test_validate_xml_config_valid(self):
        """Test validation of a correct XML configuration."""
        result = validate_xml_config(self.config_path)
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['missing_sections']), 0)

    def test_validate_xml_config_missing_section(self):
        """Test validation fails when required sections are missing."""
        invalid_xml = "<configuration><empty>True</empty></configuration>"
        path = os.path.join(self.test_dir.name, "invalid.xml")
        with open(path, 'w', encoding='utf-8') as f:
            f.write(invalid_xml)
        
        result = validate_xml_config(path)
        self.assertFalse(result['valid'])
        self.assertIn('configuration.transactionList', result['missing_sections'])

    

    def test_detect_file_type_not_found(self):
        """Test behavior when file does not exist."""
        path = os.path.join(self.test_dir.name, "non_existent.log")
        result = detect_file_type(path)
        self.assertIn("Error", result)

if __name__ == '__main__':
    unittest.main()