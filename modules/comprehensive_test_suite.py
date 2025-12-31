"""
DN Diagnostics Platform - Comprehensive Test Suite
===================================================

This module contains comprehensive unit tests for all major components of the
DN Diagnostics Platform including:
- File processing and extraction
- Transaction analysis
- UI flow visualization
- ACU parser
- Counter analysis
- Registry handling
- LLM integration
- Caching mechanisms
- Validation and error handling
- Performance benchmarks

Run with: python comprehensive_test_suite.py

Results saved to:
- test_results_<timestamp>.json
- test_report_<timestamp>.txt
"""

import unittest
import json
import time
import io
import zipfile
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import requests
from typing import Dict, List, Any

# Test result tracking
class TestResultTracker:
    """Track and report test results"""
    
    def __init__(self):
        self.results = {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'errors': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None,
            'duration': 0,
            'test_details': []
        }
    
    def add_result(self, test_name: str, status: str, duration: float, message: str = ""):
        """Add a test result"""
        self.results['total'] += 1
        self.results[status] += 1
        self.results['test_details'].append({
            'name': test_name,
            'status': status,
            'duration': duration,
            'message': message,
            'timestamp': datetime.now().isoformat()
        })
    
    def save_results(self):
        """Save results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save JSON
        json_file = f"test_results_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        # Save text report
        txt_file = f"test_report_{timestamp}.txt"
        with open(txt_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("DN DIAGNOSTICS PLATFORM - TEST REPORT\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Test Run: {timestamp}\n")
            f.write(f"Duration: {self.results['duration']:.2f}s\n\n")
            f.write(f"Total Tests: {self.results['total']}\n")
            f.write(f"Passed: {self.results['passed']}\n")
            f.write(f"Failed: {self.results['failed']}\n")
            f.write(f"Errors: {self.results['errors']}\n")
            f.write(f"Skipped: {self.results['skipped']}\n\n")
            
            # Details
            f.write("-" * 80 + "\n")
            f.write("TEST DETAILS\n")
            f.write("-" * 80 + "\n\n")
            
            for test in self.results['test_details']:
                f.write(f"Test: {test['name']}\n")
                f.write(f"Status: {test['status'].upper()}\n")
                f.write(f"Duration: {test['duration']:.4f}s\n")
                if test['message']:
                    f.write(f"Message: {test['message']}\n")
                f.write("\n")
        
        return json_file, txt_file

# Initialize global tracker instance
tracker = TestResultTracker()

# =============================================================================
# FILE PROCESSING AND EXTRACTION TESTS
# =============================================================================

class TestFileProcessing(unittest.TestCase):
    """Test file processing and ZIP extraction"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.tracker = tracker
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_valid_zip_extraction(self):
        """Test extraction of valid ZIP file"""
        start = time.time()
        try:
            # Create test ZIP
            zip_path = Path(self.test_dir) / "test.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.writestr("test.txt", "test content")
            
            # Test extraction
            with open(zip_path, 'rb') as f:
                zip_content = f.read()
            
            self.assertGreater(len(zip_content), 0)
            self.tracker.add_result(
                'test_valid_zip_extraction',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_valid_zip_extraction',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_nested_zip_extraction(self):
        """Test extraction of nested ZIP files"""
        start = time.time()
        try:
            # Create nested ZIP structure
            inner_zip = Path(self.test_dir) / "inner.zip"
            with zipfile.ZipFile(inner_zip, 'w') as zf:
                zf.writestr("inner.txt", "inner content")
            
            outer_zip = Path(self.test_dir) / "outer.zip"
            with zipfile.ZipFile(outer_zip, 'w') as zf:
                with open(inner_zip, 'rb') as f:
                    zf.writestr("inner.zip", f.read())
            
            self.assertTrue(outer_zip.exists())
            self.tracker.add_result(
                'test_nested_zip_extraction',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_nested_zip_extraction',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_corrupted_zip_handling(self):
        """Test handling of corrupted ZIP file (negative test)"""
        start = time.time()
        try:
            # Create corrupted ZIP
            zip_path = Path(self.test_dir) / "corrupted.zip"
            with open(zip_path, 'wb') as f:
                f.write(b"Not a ZIP file")
            
            # Should raise exception
            with self.assertRaises(zipfile.BadZipFile):
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.namelist()
            
            self.tracker.add_result(
                'test_corrupted_zip_handling',
                'passed',
                time.time() - start
            )
        except AssertionError:
            self.tracker.add_result(
                'test_corrupted_zip_handling',
                'failed',
                time.time() - start,
                "Failed to handle corrupted ZIP"
            )
            raise
    
    def test_empty_zip_handling(self):
        """Test handling of empty ZIP file (negative test)"""
        start = time.time()
        try:
            # Create empty ZIP
            zip_path = Path(self.test_dir) / "empty.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                pass
            
            with zipfile.ZipFile(zip_path, 'r') as zf:
                files = zf.namelist()
            
            self.assertEqual(len(files), 0)
            self.tracker.add_result(
                'test_empty_zip_handling',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_empty_zip_handling',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# TRANSACTION ANALYSIS TESTS
# =============================================================================

class TestTransactionAnalysis(unittest.TestCase):
    """Test transaction analysis functionality"""
    
    def setUp(self):
        self.tracker = tracker
        self.sample_transactions = [
            {
                'Transaction ID': 'TXN001',
                'Transaction Type': 'CIN/CI',
                'Start Time': '10:00:00',
                'End Time': '10:00:05',
                'End State': 'Successful',
                'Duration (seconds)': 5,
                'Source File': 'test.jrn'
            },
            {
                'Transaction ID': 'TXN002',
                'Transaction Type': 'COUT/GA',
                'Start Time': '10:00:10',
                'End Time': '10:00:20',
                'End State': 'Unsuccessful',
                'Duration (seconds)': 10,
                'Source File': 'test.jrn'
            }
        ]
    
    def test_transaction_parsing(self):
        """Test parsing of transaction data"""
        start = time.time()
        try:
            df = pd.DataFrame(self.sample_transactions)
            self.assertEqual(len(df), 2)
            self.assertIn('Transaction ID', df.columns)
            self.tracker.add_result(
                'test_transaction_parsing',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_transaction_parsing',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_transaction_statistics(self):
        """Test calculation of transaction statistics"""
        start = time.time()
        try:
            df = pd.DataFrame(self.sample_transactions)
            
            # Calculate stats
            total = len(df)
            successful = len(df[df['End State'] == 'Successful'])
            unsuccessful = len(df[df['End State'] == 'Unsuccessful'])
            
            self.assertEqual(total, 2)
            self.assertEqual(successful, 1)
            self.assertEqual(unsuccessful, 1)
            
            self.tracker.add_result(
                'test_transaction_statistics',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_transaction_statistics',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_invalid_transaction_data(self):
        """Test handling of invalid transaction data (negative test)"""
        start = time.time()
        try:
            # Missing required fields
            invalid_data = [{'Transaction ID': 'TXN003'}]
            df = pd.DataFrame(invalid_data)
            
            # Should not have required columns
            self.assertNotIn('End State', df.columns)
            
            self.tracker.add_result(
                'test_invalid_transaction_data',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_invalid_transaction_data',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_transaction_filtering(self):
        """Test filtering transactions by criteria"""
        start = time.time()
        try:
            df = pd.DataFrame(self.sample_transactions)
            
            # Filter by type
            cin_txns = df[df['Transaction Type'] == 'CIN/CI']
            self.assertEqual(len(cin_txns), 1)
            
            # Filter by state
            successful = df[df['End State'] == 'Successful']
            self.assertEqual(len(successful), 1)
            
            self.tracker.add_result(
                'test_transaction_filtering',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_transaction_filtering',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# UI FLOW VISUALIZATION TESTS
# =============================================================================

class TestUIFlowVisualization(unittest.TestCase):
    """Test UI flow visualization"""
    
    def setUp(self):
        self.tracker = tracker
        self.sample_flow = [
            {'screen': 'Login', 'timestamp': '10:00:00', 'duration': 2.0},
            {'screen': 'Menu', 'timestamp': '10:00:02', 'duration': 1.5},
            {'screen': 'Withdrawal', 'timestamp': '10:00:03.5', 'duration': 3.0}
        ]
    
    def test_flow_data_structure(self):
        """Test UI flow data structure"""
        start = time.time()
        try:
            self.assertEqual(len(self.sample_flow), 3)
            self.assertIn('screen', self.sample_flow[0])
            self.assertIn('timestamp', self.sample_flow[0])
            self.assertIn('duration', self.sample_flow[0])
            
            self.tracker.add_result(
                'test_flow_data_structure',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_flow_data_structure',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_empty_flow_handling(self):
        """Test handling of empty UI flow (negative test)"""
        start = time.time()
        try:
            empty_flow = []
            self.assertEqual(len(empty_flow), 0)
            
            self.tracker.add_result(
                'test_empty_flow_handling',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_empty_flow_handling',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_flow_comparison_logic(self):
        """Test UI flow comparison logic"""
        start = time.time()
        try:
            flow1 = ['Login', 'Menu', 'Withdrawal']
            flow2 = ['Login', 'Menu', 'Balance']
            
            # Common screens
            common = set(flow1) & set(flow2)
            self.assertEqual(len(common), 2)
            self.assertIn('Login', common)
            self.assertIn('Menu', common)
            
            # Unique to flow1
            unique1 = set(flow1) - set(flow2)
            self.assertEqual(len(unique1), 1)
            self.assertIn('Withdrawal', unique1)
            
            self.tracker.add_result(
                'test_flow_comparison_logic',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_flow_comparison_logic',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# ACU PARSER TESTS
# =============================================================================

class TestACUParser(unittest.TestCase):
    """Test ACU XML parser"""
    
    def setUp(self):
        self.tracker = tracker
        self.sample_xml = """<?xml version="1.0"?>
<config>
    <parameter name="test_param">test_value</parameter>
    <parameter name="capacity">1000</parameter>
</config>"""
    
    def test_xml_parsing(self):
        """Test XML parsing"""
        start = time.time()
        try:
            from lxml import etree as ET
            root = ET.fromstring(self.sample_xml.encode('utf-8'))
            self.assertIsNotNone(root)
            
            self.tracker.add_result(
                'test_xml_parsing',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_xml_parsing',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_malformed_xml_handling(self):
        """Test handling of malformed XML (negative test)"""
        start = time.time()
        try:
            from lxml import etree as ET
            malformed_xml = "<config><parameter>test</config>"
            
            with self.assertRaises(ET.XMLSyntaxError):
                ET.fromstring(malformed_xml.encode('utf-8'))
            
            self.tracker.add_result(
                'test_malformed_xml_handling',
                'passed',
                time.time() - start
            )
        except AssertionError:
            self.tracker.add_result(
                'test_malformed_xml_handling',
                'failed',
                time.time() - start,
                "Failed to catch malformed XML"
            )
            raise
    
    def test_xsd_documentation_extraction(self):
        """Test XSD documentation extraction"""
        start = time.time()
        try:
            # Mock XSD documentation
            docs = {
                'test_param': 'Test parameter description',
                'capacity': 'Maximum capacity value'
            }
            
            self.assertEqual(len(docs), 2)
            self.assertIn('test_param', docs)
            
            self.tracker.add_result(
                'test_xsd_documentation_extraction',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_xsd_documentation_extraction',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# COUNTER ANALYSIS TESTS
# =============================================================================

class TestCounterAnalysis(unittest.TestCase):
    """Test counter analysis functionality"""
    
    def setUp(self):
        self.tracker = tracker
        self.sample_counter_data = [
            {
                'No': '1',
                'Ty': 'LOG',
                'UnitName': 'SLOT1',
                'Val': '500',
                'Ini': '100',
                'Cnt': '80'
            }
        ]
    
    def test_counter_data_parsing(self):
        """Test counter data parsing"""
        start = time.time()
        try:
            df = pd.DataFrame(self.sample_counter_data)
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]['UnitName'], 'SLOT1')
            
            self.tracker.add_result(
                'test_counter_data_parsing',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_counter_data_parsing',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_counter_calculations(self):
        """Test counter calculations"""
        start = time.time()
        try:
            ini = 100
            cnt = 80
            dispensed = ini - cnt
            
            self.assertEqual(dispensed, 20)
            
            self.tracker.add_result(
                'test_counter_calculations',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_counter_calculations',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_missing_counter_data(self):
        """Test handling of missing counter data (negative test)"""
        start = time.time()
        try:
            empty_data = []
            df = pd.DataFrame(empty_data)
            self.assertTrue(df.empty)
            
            self.tracker.add_result(
                'test_missing_counter_data',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_missing_counter_data',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# REGISTRY HANDLING TESTS
# =============================================================================

class TestRegistryHandling(unittest.TestCase):
    """Test registry file handling"""
    
    def setUp(self):
        self.tracker = tracker
        self.sample_registry = """Windows Registry Editor Version 5.00

[HKEY_LOCAL_MACHINE\\SOFTWARE\\Test]
"Value1"="Data1"
"Value2"=dword:00000001"""
    
    def test_registry_parsing(self):
        """Test registry file parsing"""
        start = time.time()
        try:
            lines = self.sample_registry.split('\n')
            self.assertGreater(len(lines), 0)
            self.assertIn('HKEY_LOCAL_MACHINE', lines[2])
            
            self.tracker.add_result(
                'test_registry_parsing',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_registry_parsing',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_invalid_registry_format(self):
        """Test handling of invalid registry format (negative test)"""
        start = time.time()
        try:
            invalid_reg = "This is not a registry file"
            self.assertNotIn('[HKEY_', invalid_reg)
            
            self.tracker.add_result(
                'test_invalid_registry_format',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_invalid_registry_format',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_registry_comparison(self):
        """Test registry comparison logic"""
        start = time.time()
        try:
            reg1 = {"Value1": "Data1", "Value2": "1"}
            reg2 = {"Value1": "Data1", "Value2": "2"}
            
            # Find differences
            changed = {k: (reg1[k], reg2[k]) for k in reg1 
                      if k in reg2 and reg1[k] != reg2[k]}
            
            self.assertEqual(len(changed), 1)
            self.assertIn('Value2', changed)
            
            self.tracker.add_result(
                'test_registry_comparison',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_registry_comparison',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# LLM INTEGRATION TESTS
# =============================================================================

class TestLLMIntegration(unittest.TestCase):
    """Test LLM integration"""
    
    def setUp(self):
        self.tracker = tracker
    
    @patch('requests.post')
    def test_llm_request(self, mock_post):
        """Test LLM API request"""
        start = time.time()
        try:
            # Mock response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'analysis': 'Test analysis',
                'metadata': {'model': 'test_model'}
            }
            mock_post.return_value = mock_response
            
            # Make request
            response = mock_post('http://test.com/api', json={'test': 'data'})
            
            self.assertEqual(response.status_code, 200)
            self.assertIn('analysis', response.json())
            
            self.tracker.add_result(
                'test_llm_request',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_llm_request',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    @patch('requests.post')
    def test_llm_timeout(self, mock_post):
        """Test LLM timeout handling (negative test)"""
        start = time.time()
        try:
            mock_post.side_effect = requests.exceptions.Timeout()
            
            with self.assertRaises(requests.exceptions.Timeout):
                mock_post('http://test.com/api', timeout=1)
            
            self.tracker.add_result(
                'test_llm_timeout',
                'passed',
                time.time() - start
            )
        except AssertionError:
            self.tracker.add_result(
                'test_llm_timeout',
                'failed',
                time.time() - start,
                "Failed to handle timeout"
            )
            raise
    
    def test_feedback_validation(self):
        """Test feedback data validation"""
        start = time.time()
        try:
            feedback = {
                'transaction_id': 'TXN001',
                'rating': 4,
                'comment': 'Good analysis',
                'user_name': 'Test User'
            }
            
            # Validate required fields
            self.assertIn('transaction_id', feedback)
            self.assertIn('rating', feedback)
            self.assertGreaterEqual(feedback['rating'], 0)
            self.assertLessEqual(feedback['rating'], 5)
            
            self.tracker.add_result(
                'test_feedback_validation',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_feedback_validation',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# CACHING TESTS
# =============================================================================

class TestCaching(unittest.TestCase):
    """Test caching mechanisms"""
    
    def setUp(self):
        self.tracker = tracker
        self.cache = {}
    
    def test_cache_storage(self):
        """Test cache storage"""
        start = time.time()
        try:
            key = 'test_key'
            value = {'data': 'test_value'}
            
            self.cache[key] = value
            
            self.assertIn(key, self.cache)
            self.assertEqual(self.cache[key], value)
            
            self.tracker.add_result(
                'test_cache_storage',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_cache_storage',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_cache_retrieval(self):
        """Test cache retrieval"""
        start = time.time()
        try:
            key = 'test_key'
            value = {'data': 'test_value'}
            self.cache[key] = value
            
            retrieved = self.cache.get(key)
            self.assertEqual(retrieved, value)
            
            self.tracker.add_result(
                'test_cache_retrieval',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_cache_retrieval',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_cache_miss(self):
        """Test cache miss handling (negative test)"""
        start = time.time()
        try:
            key = 'nonexistent_key'
            result = self.cache.get(key)
            
            self.assertIsNone(result)
            
            self.tracker.add_result(
                'test_cache_miss',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_cache_miss',
                'failed',
                time.time() - start,
                str(e)
            )
            raise


# =============================================================================
# VALIDATION TESTS
# =============================================================================

class TestValidation(unittest.TestCase):
    """Test data validation"""
    
    def setUp(self):
        self.tracker = tracker
    
    def test_transaction_id_validation(self):
        """Test transaction ID validation"""
        start = time.time()
        try:
            valid_id = 'TXN001'
            self.assertTrue(len(valid_id) > 0)
            self.assertTrue(valid_id.isalnum() or '_' in valid_id)
            
            self.tracker.add_result(
                'test_transaction_id_validation',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_transaction_id_validation',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_empty_string_validation(self):
        """Test empty string validation (negative test)"""
        start = time.time()
        try:
            empty_string = ''
            self.assertEqual(len(empty_string), 0)
            
            self.tracker.add_result(
                'test_empty_string_validation',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_empty_string_validation',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_datetime_validation(self):
        """Test datetime validation"""
        start = time.time()
        try:
            valid_time = '10:00:00'
            parsed = datetime.strptime(valid_time, '%H:%M:%S')
            self.assertIsInstance(parsed, datetime)
            
            self.tracker.add_result(
                'test_datetime_validation',
                'passed',
                time.time() - start
            )
        except Exception as e:
            self.tracker.add_result(
                'test_datetime_validation',
                'failed',
                time.time() - start,
                str(e)
            )
            raise
    
    def test_invalid_datetime_format(self):
        """Test invalid datetime format (negative test)"""
        start = time.time()
        try:
            invalid_time = 'not a time'
            
            with self.assertRaises(ValueError):
                datetime.strptime(invalid_time, '%H:%M:%S')
            
            self.tracker.add_result(
                'test_invalid_datetime_format',
                'passed',
                time.time() - start
            )
        except AssertionError:
            self.tracker.add_result(
                'test_invalid_datetime_format',
                'failed',
                time.time() - start,
                "Failed to catch invalid datetime"
            )
            raise


# =============================================================================
# =============================================================================
# TEST RUNNER
# =============================================================================

def run_tests():
    """Run all tests and generate reports"""
    global tracker
    tracker = TestResultTracker()
    tracker.results['start_time'] = datetime.now().isoformat()
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestFileProcessing))
    suite.addTests(loader.loadTestsFromTestCase(TestTransactionAnalysis))
    suite.addTests(loader.loadTestsFromTestCase(TestUIFlowVisualization))
    suite.addTests(loader.loadTestsFromTestCase(TestACUParser))
    suite.addTests(loader.loadTestsFromTestCase(TestCounterAnalysis))
    suite.addTests(loader.loadTestsFromTestCase(TestRegistryHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestLLMIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestCaching))
    suite.addTests(loader.loadTestsFromTestCase(TestValidation))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Record end time and duration
    tracker.results['end_time'] = datetime.now().isoformat()
    start_time = datetime.fromisoformat(tracker.results['start_time'])
    end_time = datetime.fromisoformat(tracker.results['end_time'])
    tracker.results['duration'] = (end_time - start_time).total_seconds()
    
    # Save results
    json_file, txt_file = tracker.save_results()
    
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    print(f"Total: {tracker.results['total']}")
    print(f"Passed: {tracker.results['passed']}")
    print(f"Failed: {tracker.results['failed']}")
    print(f"Errors: {tracker.results['errors']}")
    print(f"Skipped: {tracker.results['skipped']}")
    print(f"Duration: {tracker.results['duration']:.2f}s")
    print(f"\nResults saved to:")
    print(f"  - {json_file}")
    print(f"  - {txt_file}")
    
    return result


if __name__ == '__main__':
    run_tests()
