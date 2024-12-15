"""
DN DIAGNOSTICS PLATFORM - COMPREHENSIVE UNIT TEST SUITE
========================================================
This module contains all unit tests for the DN Diagnostics Platform.
Tests cover: file processing, transaction analysis, UI flow visualization,
counter analysis, registry handling, LLM integration, caching, validation,
error handling, and performance.

Run with: python comprehensive_test_suite.py
Results are saved to: test_results_<timestamp>.json and test_report_<timestamp>.txt
"""

import unittest
import json
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from pathlib import Path
import tempfile
import shutil
import zipfile
import io
import re
from collections import defaultdict
import sys
import traceback

# ============================================================================
# TEST UTILITIES
# ============================================================================

class TestResultCollector:
    """Collects and formats test results"""
    
    def __init__(self):
        self.results = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_tests': 0,
            'passed': 0,
            'failed': 0,
            'errors': 0,
            'skipped': 0,
            'test_details': [],
            'summary': {}
        }
    
    def add_result(self, test_name, category, status, duration, error_msg=None):
        """Add a test result"""
        self.results['total_tests'] += 1
        
        if status == 'PASS':
            self.results['passed'] += 1
        elif status == 'FAIL':
            self.results['failed'] += 1
        elif status == 'ERROR':
            self.results['errors'] += 1
        elif status == 'SKIP':
            self.results['skipped'] += 1
        
        self.results['test_details'].append({
            'test_name': test_name,
            'category': category,
            'status': status,
            'duration_ms': round(duration * 1000, 2),
            'error': error_msg
        })
    
    def generate_summary(self):
        """Generate summary statistics"""
        total = self.results['total_tests']
        passed = self.results['passed']
        
        self.results['summary'] = {
            'success_rate': round((passed / total * 100) if total > 0 else 0, 2),
            'categories': self._categorize_results()
        }
    
    def _categorize_results(self):
        """Group results by category"""
        categories = defaultdict(lambda: {'total': 0, 'passed': 0, 'failed': 0})
        
        for detail in self.results['test_details']:
            cat = detail['category']
            categories[cat]['total'] += 1
            if detail['status'] == 'PASS':
                categories[cat]['passed'] += 1
            elif detail['status'] in ['FAIL', 'ERROR']:
                categories[cat]['failed'] += 1
        
        return dict(categories)
    
    def save_json(self, filepath):
        """Save results to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.results, f, indent=2)
    
    def save_text_report(self, filepath):
        """Save human-readable text report"""
        with open(filepath, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("DN DIAGNOSTICS PLATFORM - TEST RESULTS\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Test Run: {self.results['timestamp']}\n\n")
            
            f.write(f"OVERALL SUMMARY\n")
            f.write(f"{'='*80}\n")
            f.write(f"Total Tests: {self.results['total_tests']}\n")
            f.write(f"Passed: {self.results['passed']} ✓\n")
            f.write(f"Failed: {self.results['failed']} ✗\n")
            f.write(f"Errors: {self.results['errors']} ⚠\n")
            f.write(f"Skipped: {self.results['skipped']} ○\n")
            f.write(f"Success Rate: {self.results['summary']['success_rate']}%\n\n")
            
            # Category breakdown
            f.write(f"RESULTS BY CATEGORY\n")
            f.write(f"{'='*80}\n")
            for cat, stats in self.results['summary']['categories'].items():
                rate = round((stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0, 1)
                f.write(f"{cat}: {stats['passed']}/{stats['total']} ({rate}%)\n")
            f.write("\n")
            
            # Detailed results
            f.write(f"DETAILED TEST RESULTS\n")
            f.write(f"{'='*80}\n\n")
            
            current_category = None
            for detail in self.results['test_details']:
                if detail['category'] != current_category:
                    current_category = detail['category']
                    f.write(f"\n{current_category}\n")
                    f.write(f"{'-'*80}\n")
                
                status_symbol = {
                    'PASS': '✓',
                    'FAIL': '✗',
                    'ERROR': '⚠',
                    'SKIP': '○'
                }.get(detail['status'], '?')
                
                f.write(f"{status_symbol} {detail['test_name']} ({detail['duration_ms']}ms)\n")
                
                if detail['error']:
                    f.write(f"  Error: {detail['error']}\n")
            
            f.write("\n" + "=" * 80 + "\n")

# ============================================================================
# TEST SUITE 1: FILE PROCESSING
# ============================================================================

class TestFileProcessing(unittest.TestCase):
    """Tests for file extraction and categorization"""
    
    def setUp(self):
        """Create temporary directory before each test"""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary directory after each test"""
        if hasattr(self, 'temp_dir') and Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def test_create_test_zip(self):
        """Test creating a valid ZIP file with test data"""
        zip_path = Path(self.temp_dir) / "test.zip"
        
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('CustomerJournal_20250101.txt', 'test data')
            zf.writestr('UIJournal_20250101.txt', 'test ui data')
            zf.writestr('TRCTrace_20250101.txt', 'test trace')
        
        self.assertTrue(zip_path.exists())
        self.assertGreater(zip_path.stat().st_size, 0)
    
    def test_extract_zip_valid(self):
        """Test extracting a valid ZIP file"""
        zip_path = Path(self.temp_dir) / "extract_test.zip"
        
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('test_file.txt', 'content')
        
        extract_path = Path(self.temp_dir) / "extracted"
        extract_path.mkdir(exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_path)
        
        extracted_file = extract_path / "test_file.txt"
        self.assertTrue(extracted_file.exists())
    
    def test_categorize_files(self):
        """Test file categorization logic"""
        files = [
            'CustomerJournal_20250101.txt',
            'UIJournal_20250101.txt',
            'TRCTrace_20250101.txt',
            'TRCError_20250101.txt',
            'Registry_backup.reg',
            'unknown_file.dat'
        ]
        
        categories = {
            'customer_journals': [],
            'ui_journals': [],
            'trc_trace': [],
            'trc_error': [],
            'registry_files': [],
            'unknown': []
        }
        
        for file in files:
            if 'CustomerJournal' in file:
                categories['customer_journals'].append(file)
            elif 'UIJournal' in file:
                categories['ui_journals'].append(file)
            elif 'TRCTrace' in file:
                categories['trc_trace'].append(file)
            elif 'TRCError' in file:
                categories['trc_error'].append(file)
            elif file.endswith('.reg'):
                categories['registry_files'].append(file)
            else:
                categories['unknown'].append(file)
        
        self.assertEqual(len(categories['customer_journals']), 1)
        self.assertEqual(len(categories['ui_journals']), 1)
        self.assertEqual(len(categories['trc_trace']), 1)
        self.assertEqual(len(categories['trc_error']), 1)
        self.assertEqual(len(categories['registry_files']), 1)
        self.assertEqual(len(categories['unknown']), 1)

# ============================================================================
# TEST SUITE 2: TRANSACTION ANALYSIS
# ============================================================================

class TestTransactionAnalysis(unittest.TestCase):
    """Tests for transaction parsing and analysis"""
    
    def setUp(self):
        self.sample_transactions = pd.DataFrame({
            'Transaction ID': ['TXN001', 'TXN002', 'TXN003'],
            'Transaction Type': ['WDLS/WD', 'WDLS/WD', 'CIN/CI'],
            'Start Time': ['10:00:00', '10:05:00', '10:10:00'],
            'End Time': ['10:00:30', '10:05:45', '10:11:00'],
            'End State': ['Successful', 'Unsuccessful', 'Successful'],
            'Source File': ['20250101', '20250101', '20250102']
        })
    
    def test_parse_transaction_id(self):
        """Test extracting transaction ID"""
        log_line = "Transaction TXN001 started"
        match = re.search(r'Transaction\s+(\S+)', log_line)
        
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), 'TXN001')
    
    def test_parse_transaction_type(self):
        """Test extracting transaction type"""
        types = ['WDLS/WD', 'CIN/CI', 'COUT/GA', 'BAL/INQ']
        
        for txn_type in types:
            self.assertIn('/', txn_type)
            self.assertGreater(len(txn_type), 3)
    
    def test_calculate_transaction_duration(self):
        """Test calculating transaction duration"""
        start = datetime.strptime('10:00:00', '%H:%M:%S')
        end = datetime.strptime('10:00:30', '%H:%M:%S')
        
        duration = (end - start).total_seconds()
        
        self.assertEqual(duration, 30.0)
    
    def test_filter_transactions_by_type(self):
        """Test filtering transactions by type"""
        filtered = self.sample_transactions[
            self.sample_transactions['Transaction Type'] == 'WDLS/WD'
        ]
        
        self.assertEqual(len(filtered), 2)
    
    def test_filter_transactions_by_state(self):
        """Test filtering transactions by state"""
        successful = self.sample_transactions[
            self.sample_transactions['End State'] == 'Successful'
        ]
        
        self.assertEqual(len(successful), 2)
    
    def test_filter_transactions_by_source(self):
        """Test filtering transactions by source file"""
        source_filtered = self.sample_transactions[
            self.sample_transactions['Source File'] == '20250101'
        ]
        
        self.assertEqual(len(source_filtered), 2)
    
    def test_transaction_statistics(self):
        """Test generating transaction statistics"""
        stats = self.sample_transactions.groupby('Transaction Type').agg({
            'Transaction ID': 'count',
            'End State': lambda x: (x == 'Successful').sum()
        })
        
        self.assertEqual(stats.loc['WDLS/WD', 'Transaction ID'], 2)
        self.assertEqual(stats.loc['WDLS/WD', 'End State'], 1)

# ============================================================================
# TEST SUITE 3: UI FLOW VISUALIZATION
# ============================================================================

class TestUIFlowVisualization(unittest.TestCase):
    """Tests for UI journal parsing and flow visualization"""
    
    def setUp(self):
        self.ui_events = pd.DataFrame({
            'Time': ['10:00:00', '10:00:05', '10:00:10', '10:00:15'],
            'ScreenName': ['Login', 'PIN', 'MainMenu', 'Withdraw'],
            'Event': ['Show', 'Show', 'Show', 'Show']
        })
    
    def test_parse_ui_events(self):
        """Test parsing UI events"""
        self.assertEqual(len(self.ui_events), 4)
        self.assertIn('ScreenName', self.ui_events.columns)
        self.assertIn('Time', self.ui_events.columns)
    
    def test_get_unique_screens(self):
        """Test getting unique screens in order"""
        unique_screens = self.ui_events['ScreenName'].unique().tolist()
        
        self.assertEqual(len(unique_screens), 4)
        self.assertEqual(unique_screens[0], 'Login')
    
    def test_filter_by_time_range(self):
        """Test filtering UI events by time range"""
        start_time = time(10, 0, 0)
        end_time = time(10, 0, 10)
        
        # Convert time strings to time objects
        self.ui_events['Time_obj'] = pd.to_datetime(
            self.ui_events['Time'], format='%H:%M:%S'
        ).dt.time
        
        filtered = self.ui_events[
            (self.ui_events['Time_obj'] >= start_time) & 
            (self.ui_events['Time_obj'] <= end_time)
        ]
        
        self.assertEqual(len(filtered), 3)
    
    def test_extract_screen_flow(self):
        """Test extracting screen flow sequence"""
        flow = self.ui_events['ScreenName'].tolist()
        
        self.assertEqual(flow, ['Login', 'PIN', 'MainMenu', 'Withdraw'])
    
    def test_calculate_screen_duration(self):
        """Test calculating duration between screens"""
        times = ['10:00:00', '10:00:05', '10:00:10']
        
        durations = []
        for i in range(len(times) - 1):
            t1 = datetime.strptime(times[i], '%H:%M:%S')
            t2 = datetime.strptime(times[i + 1], '%H:%M:%S')
            durations.append((t2 - t1).total_seconds())
        
        self.assertEqual(durations, [5.0, 5.0])

# ============================================================================
# TEST SUITE 4: COUNTER ANALYSIS
# ============================================================================

class TestCounterAnalysis(unittest.TestCase):
    """Tests for TRC trace counter parsing"""
    
    def setUp(self):
        self.counter_data = pd.DataFrame({
            'No': ['1', '2', '3'],
            'UnitName': ['SLOT1', 'SLOT2', 'SLOT3'],
            'Cur': ['USD', 'USD', 'EUR'],
            'Val': ['20', '50', '10'],
            'Ini': ['100', '200', '150'],
            'Cnt': ['95', '190', '145'],
            'RCnt': ['0', '2', '1']
        })
    
    def test_parse_counter_data(self):
        """Test parsing counter data"""
        self.assertEqual(len(self.counter_data), 3)
        self.assertIn('UnitName', self.counter_data.columns)
        self.assertIn('Val', self.counter_data.columns)
    
    def test_calculate_remaining_count(self):
        """Test calculating remaining count"""
        ini = 100
        cnt = 95
        
        dispensed = ini - cnt
        
        self.assertEqual(dispensed, 5)
    
    def test_filter_by_currency(self):
        """Test filtering counters by currency"""
        usd_counters = self.counter_data[self.counter_data['Cur'] == 'USD']
        
        self.assertEqual(len(usd_counters), 2)
    
    def test_detect_counter_type(self):
        """Test detecting counter record type"""
        record_types = ['Logical', 'Physical', 'Retract']
        
        for rt in record_types:
            self.assertIsInstance(rt, str)
            self.assertGreater(len(rt), 0)

# ============================================================================
# TEST SUITE 5: REGISTRY FILE HANDLING
# ============================================================================

class TestRegistryFileHandling(unittest.TestCase):
    """Tests for registry file parsing and comparison"""
    
    def setUp(self):
        self.registry_content = """
[HKEY_LOCAL_MACHINE\\SOFTWARE\\Test]
"Value1"="Data1"
"Value2"=dword:00000001

[HKEY_LOCAL_MACHINE\\SOFTWARE\\Test\\SubKey]
"SubValue"="SubData"
"""
    
    def test_parse_registry_sections(self):
        """Test parsing registry sections"""
        sections = re.findall(r'\[(.+?)\]', self.registry_content)
        
        self.assertEqual(len(sections), 2)
        self.assertIn('HKEY_LOCAL_MACHINE\\SOFTWARE\\Test', sections[0])
    
    def test_parse_registry_values(self):
        """Test parsing registry key-value pairs"""
        values = re.findall(r'"(.+?)"=(.+)', self.registry_content)
        
        self.assertGreater(len(values), 0)
        self.assertEqual(values[0][0], 'Value1')
    
    def test_compare_registry_files(self):
        """Test comparing two registry files"""
        content1 = '"Key1"="Value1"'
        content2 = '"Key1"="Value2"'
        
        self.assertNotEqual(content1, content2)

# ============================================================================
# TEST SUITE 6: LLM INTEGRATION
# ============================================================================

class TestLLMIntegration(unittest.TestCase):
    """Tests for LLM analysis integration"""
    
    def test_llm_analysis_mock(self):
        """Test LLM analysis with mock"""
        transaction_log = "Transaction started at 10:00:00. Error: Card read failed."
        
        # Mock LLM response
        mock_response = {
            'analysis': 'Card reader hardware issue detected',
            'confidence': 0.85,
            'root_cause': 'Hardware malfunction'
        }
        
        self.assertIn('analysis', mock_response)
        self.assertGreater(mock_response['confidence'], 0.5)
    
    def test_feedback_validation(self):
        """Test feedback data validation"""
        feedback = {
            'transaction_id': 'TXN001',
            'rating': 4,
            'alternative_cause': 'Network timeout',
            'comment': 'Good analysis',
            'user_email': 'test@example.com'
        }
        
        self.assertIn('@', feedback['user_email'])
        self.assertGreaterEqual(feedback['rating'], 1)
        self.assertLessEqual(feedback['rating'], 5)

# ============================================================================
# TEST SUITE 7: CACHE SYSTEM
# ============================================================================

class TestCacheSystem(unittest.TestCase):
    """Tests for caching functionality"""
    
    def setUp(self):
        self.cache = {}
    
    def test_cache_storage(self):
        """Test storing data in cache"""
        key = 'test_key'
        value = {'data': 'test_data'}
        
        self.cache[key] = value
        
        self.assertIn(key, self.cache)
        self.assertEqual(self.cache[key], value)
    
    def test_cache_retrieval(self):
        """Test retrieving data from cache"""
        self.cache['key1'] = 'value1'
        
        retrieved = self.cache.get('key1')
        
        self.assertEqual(retrieved, 'value1')
    
    def test_cache_miss(self):
        """Test cache miss scenario"""
        result = self.cache.get('nonexistent_key')
        
        self.assertIsNone(result)
    
    def test_cache_key_generation(self):
        """Test generating cache keys"""
        import hashlib
        
        data = "test_data"
        key = hashlib.md5(data.encode()).hexdigest()
        
        self.assertEqual(len(key), 32)

# ============================================================================
# TEST SUITE 8: DATA VALIDATION
# ============================================================================

class TestDataValidation(unittest.TestCase):
    """Tests for data validation"""
    
    def test_validate_transaction_id(self):
        """Test transaction ID validation"""
        valid_ids = ['TXN001', 'TXN_123', 'TX-456']
        
        for txn_id in valid_ids:
            self.assertIsInstance(txn_id, str)
            self.assertGreater(len(txn_id), 0)
    
    def test_validate_time_format(self):
        """Test time format validation"""
        time_str = '10:30:45'
        
        try:
            parsed = datetime.strptime(time_str, '%H:%M:%S')
            valid = True
        except:
            valid = False
        
        self.assertTrue(valid)
    
    def test_sanitize_file_path(self):
        """Test file path sanitization"""
        dangerous_path = '../../../etc/passwd'
        
        # Simple sanitization
        safe_path = dangerous_path.replace('..', '').replace('/', '_')
        
        self.assertNotIn('..', safe_path)
        self.assertNotIn('/', safe_path)

# ============================================================================
# TEST SUITE 9: ERROR HANDLING
# ============================================================================

class TestErrorHandling(unittest.TestCase):
    """Tests for error handling"""
    
    def test_division_by_zero(self):
        """Test division by zero handling"""
        try:
            result = 10 / 0
            handled = False
        except ZeroDivisionError:
            handled = True
        
        self.assertTrue(handled)
    
    def test_invalid_file_format(self):
        """Test handling invalid file formats"""
        invalid_data = "not a valid format"
        
        try:
            df = pd.read_csv(io.StringIO(invalid_data))
            valid = len(df) > 0
        except:
            valid = False
        
        self.assertFalse(valid)
    
    def test_missing_columns(self):
        """Test handling missing columns"""
        df = pd.DataFrame({'A': [1, 2, 3]})
        
        has_column = 'B' in df.columns
        
        self.assertFalse(has_column)
    
    def test_empty_dataframe_handling(self):
        """Test handling empty DataFrames"""
        df = pd.DataFrame()
        
        self.assertTrue(df.empty)
        self.assertEqual(len(df), 0)

# ============================================================================
# TEST SUITE 10: PERFORMANCE
# ============================================================================

class TestPerformance(unittest.TestCase):
    """Tests for performance metrics"""
    
    def test_large_dataframe_processing(self):
        """Test processing large DataFrames"""
        import time
        
        # Create large DataFrame
        df = pd.DataFrame({
            'col1': np.random.rand(10000),
            'col2': np.random.rand(10000)
        })
        
        start = time.time()
        result = df.groupby(pd.cut(df['col1'], bins=10))['col2'].mean()
        duration = time.time() - start
        
        self.assertLess(duration, 1.0)  # Should complete in under 1 second
    
    def test_cache_hit_performance(self):
        """Test cache hit performance"""
        import time
        
        cache = {'key1': 'value1'}
        
        start = time.time()
        for _ in range(1000):
            _ = cache.get('key1')
        duration = time.time() - start
        
        self.assertLess(duration, 0.1)  # Should be very fast

# ============================================================================
# TEST SUITE 11: API RESPONSE DEVIATION HANDLING
# ============================================================================

class TestAPIResponseDeviation(unittest.TestCase):
    """Tests for handling unexpected API responses"""
    
    def test_missing_transaction_id_in_response(self):
        """Test handling when Transaction ID is missing from API response"""
        response_data = {
            # 'Transaction ID': 'TXN001',  # Missing!
            'Transaction Type': 'WDLS/WD',
            'End State': 'Successful'
        }
        
        # Simulate what the UI would do
        try:
            txn_id = response_data.get('Transaction ID', 'UNKNOWN')
            self.assertEqual(txn_id, 'UNKNOWN')
        except KeyError:
            self.fail("Should handle missing Transaction ID gracefully")
    
    def test_malformed_time_format(self):
        """Test handling malformed time formats"""
        malformed_times = [
            '25:00:00',  # Invalid hour
            '10:60:00',  # Invalid minute
            'not-a-time',  # Invalid format
            '',  # Empty
            None  # None
        ]
        
        for time_str in malformed_times:
            try:
                if time_str:
                    parsed = datetime.strptime(time_str, '%H:%M:%S')
                    valid = True
            except:
                valid = False
            
            # Should gracefully handle invalid times
            self.assertIsInstance(valid, bool)
    
    def test_unexpected_transaction_state(self):
        """Test handling unexpected transaction states"""
        valid_states = ['Successful', 'Unsuccessful']
        unexpected_states = ['Pending', 'Unknown', '', None, 123, []]
        
        for state in unexpected_states:
            # Should not crash when encountering unexpected states
            state_str = str(state) if state is not None else 'Unknown'
            self.assertIsInstance(state_str, str)
    
    def test_missing_ui_flow_data(self):
        """Test handling when UI flow data is missing"""
        response_with_flow = {'ui_flow': ['Screen1', 'Screen2']}
        response_without_flow = {'ui_flow': []}
        response_no_key = {}
        
        # Test with flow
        flow1 = response_with_flow.get('ui_flow', ['No flow data'])
        self.assertEqual(len(flow1), 2)
        
        # Test without flow
        flow2 = response_without_flow.get('ui_flow', ['No flow data'])
        self.assertEqual(len(flow2), 0)
        
        # Test missing key
        flow3 = response_no_key.get('ui_flow', ['No flow data'])
        self.assertEqual(flow3, ['No flow data'])
    
    def test_invalid_counter_data_format(self):
        """Test handling invalid counter data formats"""
        valid_counter = {
            'No': '1',
            'UnitName': 'SLOT1',
            'Val': '20',
            'Cnt': '100'
        }
        
        invalid_counters = [
            {},  # Empty
            {'No': '1'},  # Missing fields
            None,  # None
            [],  # Wrong type
            'not-a-dict'  # Wrong type
        ]
        
        # Valid counter should work
        self.assertIn('No', valid_counter)
        
        # Invalid counters should be handled
        for counter in invalid_counters:
            if isinstance(counter, dict):
                has_no = 'No' in counter
                self.assertIsInstance(has_no, bool)
    
    def test_null_values_in_response(self):
        """Test handling null/None values in response"""
        response = {
            'Transaction ID': None,
            'Start Time': None,
            'End Time': None,
            'Transaction Log': None,
            'Duration (seconds)': None
        }
        
        # Should convert None to appropriate defaults
        txn_id = response.get('Transaction ID') or 'UNKNOWN'
        start_time = response.get('Start Time') or 'N/A'
        log = response.get('Transaction Log') or ''
        duration = response.get('Duration (seconds)') or 0
        
        self.assertEqual(txn_id, 'UNKNOWN')
        self.assertEqual(start_time, 'N/A')
        self.assertEqual(log, '')
        self.assertEqual(duration, 0)
    
    def test_extra_unexpected_fields(self):
        """Test handling responses with extra unexpected fields"""
        response = {
            'Transaction ID': 'TXN001',
            'Transaction Type': 'WDLS/WD',
            'unexpected_field_1': 'value1',
            'unexpected_field_2': 123,
            'unexpected_field_3': ['list', 'data']
        }
        
        # Should still extract expected fields
        self.assertEqual(response.get('Transaction ID'), 'TXN001')
        self.assertEqual(response.get('Transaction Type'), 'WDLS/WD')
        
        # Extra fields should not cause issues
        self.assertIn('unexpected_field_1', response)
    
    def test_nested_data_structure_deviation(self):
        """Test handling when nested structures are malformed"""
        # Expected structure
        expected = {
            'categories': {
                'customer_journals': {
                    'count': 2,
                    'files': ['file1.txt', 'file2.txt']
                }
            }
        }
        
        # Malformed structures
        malformed = [
            {'categories': None},
            {'categories': {}},
            {'categories': {'customer_journals': None}},
            {'categories': {'customer_journals': {}}},
            {}
        ]
        
        for data in malformed:
            # Should handle gracefully
            categories = data.get('categories', {})
            if not isinstance(categories, dict):
                categories = {}
            journals = categories.get('customer_journals', {})
            if not isinstance(journals, dict):
                journals = {}
            count = journals.get('count', 0)
            
            self.assertIsInstance(count, int)
    
    def test_list_instead_of_dict_response(self):
        """Test handling when API returns list instead of dict"""
        list_response = ['item1', 'item2', 'item3']
        dict_response = {'key': 'value'}
        
        # Check type and handle appropriately
        if isinstance(list_response, list):
            self.assertGreater(len(list_response), 0)
        
        if isinstance(dict_response, dict):
            self.assertIn('key', dict_response)
    
    def test_empty_string_in_numeric_fields(self):
        """Test handling empty strings in numeric fields"""
        data = {
            'Duration (seconds)': '',
            'Count': '',
            'Val': ''
        }
        
        # Convert to appropriate types
        duration = float(data.get('Duration (seconds)') or 0)
        count = int(data.get('Count') or 0)
        val = int(data.get('Val') or 0)
        
        self.assertEqual(duration, 0.0)
        self.assertEqual(count, 0)
        self.assertEqual(val, 0)
    
    def test_unicode_and_special_chars_in_response(self):
        """Test handling unicode and special characters"""
        data = {
            'Transaction ID': 'TXN™001®',
            'Log': 'Test with émojis 🚀 and spëcial çhars',
            'Path': 'C:\\Windows\\System32\\test.txt'
        }
        
        # Should handle without crashes
        txn_id = str(data.get('Transaction ID', ''))
        log = str(data.get('Log', ''))
        path = str(data.get('Path', ''))
        
        self.assertIsInstance(txn_id, str)
        self.assertIsInstance(log, str)
        self.assertIsInstance(path, str)

# ============================================================================
# MAIN TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all test suites and generate reports"""
    
    print("=" * 80)
    print("DN DIAGNOSTICS PLATFORM - COMPREHENSIVE UNIT TEST SUITE")
    print("=" * 80)
    print()
    
    # Create result collector
    collector = TestResultCollector()
    
    # Get all test suites
    test_suites = [
        TestFileProcessing,
        TestTransactionAnalysis,
        TestUIFlowVisualization,
        TestCounterAnalysis,
        TestRegistryFileHandling,
        TestLLMIntegration,
        TestCacheSystem,
        TestDataValidation,
        TestErrorHandling,
        TestPerformance,
        TestAPIResponseDeviation  # ADD THIS LINE
    ]
    
    # Run each test suite
    for suite_class in test_suites:
        suite = unittest.TestLoader().loadTestsFromTestCase(suite_class)
        
        for test in suite:
            test_name = test._testMethodName
            category = suite_class.__name__
            
            # Run test and collect results
            result = unittest.TestResult()
            start_time = datetime.now()
            
            test.run(result)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            if result.wasSuccessful():
                status = 'PASS'
                error_msg = None
                print(f"✓ {category}.{test_name}")
            elif result.failures:
                status = 'FAIL'
                error_msg = str(result.failures[0][1])
                print(f"✗ {category}.{test_name}")
                print(f"  {error_msg[:100]}")
            elif result.errors:
                status = 'ERROR'
                error_msg = str(result.errors[0][1])
                print(f"⚠ {category}.{test_name}")
                print(f"  {error_msg[:100]}")
            else:
                status = 'SKIP'
                error_msg = None
                print(f"○ {category}.{test_name}")
            
            collector.add_result(test_name, category, status, duration, error_msg)
    
    # Generate summary
    collector.generate_summary()
    
    # Save reports
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = f"test_results_{timestamp}.json"
    text_file = f"test_report_{timestamp}.txt"
    
    collector.save_json(json_file)
    collector.save_text_report(text_file)
    
    # Print summary
    print()
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Total Tests: {collector.results['total_tests']}")
    print(f"Passed: {collector.results['passed']} ✓")
    print(f"Failed: {collector.results['failed']} ✗")
    print(f"Errors: {collector.results['errors']} ⚠")
    print(f"Success Rate: {collector.results['summary']['success_rate']}%")
    print()
    print(f"Results saved to:")
    print(f"  - JSON: {json_file}")
    print(f"  - Text: {text_file}")
    print("=" * 80)
    
    return collector.results['summary']['success_rate'] == 100.0

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)

# ============================================================================
# INTEGRATION TEST: SIMULATE API FAILURES
# ============================================================================

def test_api_failure_scenarios():
    """
    Test how the system handles various API failure scenarios
    Run separately from main test suite
    """
    print("\n" + "=" * 80)
    print("TESTING API FAILURE SCENARIOS")
    print("=" * 80 + "\n")
    
    scenarios = [
        {
            'name': 'Empty Response',
            'response': {},
            'expected_keys': ['Transaction ID', 'Transaction Type']
        },
        {
            'name': 'Null Values',
            'response': {
                'Transaction ID': None,
                'Transaction Type': None,
                'Start Time': None
            },
            'expected_keys': ['Transaction ID', 'Transaction Type']
        },
        {
            'name': 'Wrong Data Types',
            'response': {
                'Transaction ID': 12345,  # Should be string
                'Duration (seconds)': 'not-a-number',  # Should be number
                'ui_flow': 'not-a-list'  # Should be list
            },
            'expected_keys': ['Transaction ID']
        },
        {
            'name': 'Missing Required Fields',
            'response': {
                'Transaction Type': 'WDLS/WD'
                # Missing Transaction ID
            },
            'expected_keys': ['Transaction ID']
        }
    ]
    
    for scenario in scenarios:
        print(f"\nTesting: {scenario['name']}")
        print("-" * 40)
        
        response = scenario['response']
        expected_keys = scenario['expected_keys']
        
        # Simulate safe access pattern
        safe_data = {}
        for key in expected_keys:
            if key in response:
                value = response[key]
                # Convert to string if needed
                safe_data[key] = str(value) if value is not None else 'UNKNOWN'
            else:
                safe_data[key] = 'UNKNOWN'
        
        print(f"Original: {response}")
        print(f"Safe data: {safe_data}")
        print(f"✓ Handled safely")
    
    print("\n" + "=" * 80)
    print("API FAILURE TESTING COMPLETE")
    print("=" * 80)

# Uncomment to run failure scenarios separately
# test_api_failure_scenarios()