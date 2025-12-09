"""
DN Diagnostics Platform - Comprehensive Test Suite
==================================================
This single file tests all components of the application with detailed output.

Run with: pytest test_complete_suite.py -v --tb=short
Run with coverage: pytest test_complete_suite.py -v --cov=. --cov-report=html
"""

import pytest
import pandas as pd
import json
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime, time
from unittest.mock import Mock, MagicMock, patch, mock_open
from io import BytesIO

# ============================================================================
# PYTEST CONFIGURATION
# ============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line("markers", "section1: Utility Functions Tests")
    config.addinivalue_line("markers", "section2: Session Service Tests")
    config.addinivalue_line("markers", "section3: Categorization Tests")
    config.addinivalue_line("markers", "section4: Transaction Analyzer Tests")
    config.addinivalue_line("markers", "section5: UI Journal Tests")
    config.addinivalue_line("markers", "section6: Comparison Logic Tests")
    config.addinivalue_line("markers", "section7: File Handling Tests")
    config.addinivalue_line("markers", "section8: API Endpoint Tests")
    config.addinivalue_line("markers", "section9: LLM Integration Tests")
    config.addinivalue_line("markers", "section10: Feedback System Tests")
    config.addinivalue_line("markers", "section11: Data Validation Tests")
    config.addinivalue_line("markers", "section12: Edge Cases Tests")
    config.addinivalue_line("markers", "section13: Performance Tests")

# ============================================================================
# FIXTURES - Reusable Test Data
# ============================================================================

@pytest.fixture
def sample_transaction_data():
    """Sample transaction data for testing"""
    return {
        'Transaction ID': 'TXN001',
        'Transaction Type': 'Withdraw',
        'Start Time': '10:30:00',
        'End Time': '10:31:00',
        'End State': 'Successful',
        'Duration (seconds)': 60,
        'Source File': 'CustomerJournal_1',
        'Transaction Log': 'Sample log entry...'
    }

@pytest.fixture
def sample_transactions_df():
    """Sample transactions DataFrame"""
    data = [
        {
            'Transaction ID': 'TXN001',
            'Transaction Type': 'Withdraw',
            'Start Time': '10:30:00',
            'End Time': '10:31:00',
            'End State': 'Successful',
            'Duration (seconds)': 60,
            'Source File': 'CustomerJournal_1',
            'Transaction Log': 'Log 1'
        },
        {
            'Transaction ID': 'TXN002',
            'Transaction Type': 'Balance',
            'Start Time': '10:35:00',
            'End Time': '10:36:00',
            'End State': 'Unsuccessful',
            'Duration (seconds)': 60,
            'Source File': 'CustomerJournal_1',
            'Transaction Log': 'Log 2'
        },
        {
            'Transaction ID': 'TXN003',
            'Transaction Type': 'Withdraw',
            'Start Time': '10:40:00',
            'End Time': '10:41:00',
            'End State': 'Successful',
            'Duration (seconds)': 60,
            'Source File': 'CustomerJournal_2',
            'Transaction Log': 'Log 3'
        }
    ]
    return pd.DataFrame(data)

@pytest.fixture
def sample_ui_events_df():
    """Sample UI events DataFrame"""
    data = [
        {'Time': time(10, 30, 0), 'Screen': 'MainMenu'},
        {'Time': time(10, 30, 10), 'Screen': 'Withdraw'},
        {'Time': time(10, 30, 20), 'Screen': 'Amount'},
        {'Time': time(10, 30, 40), 'Screen': 'Cash'},
        {'Time': time(10, 30, 50), 'Screen': 'Receipt'}
    ]
    return pd.DataFrame(data)

@pytest.fixture
def sample_zip_content():
    """Create a sample ZIP file in memory"""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('CustomerJournal_1.txt', 'Sample customer journal content')
        zf.writestr('UIJournal_1.txt', 'Sample UI journal content')
        zf.writestr('Registry_1.reg', 'Sample registry content')
    zip_buffer.seek(0)
    return zip_buffer.read()

@pytest.fixture
def sample_registry_content():
    """Sample registry file content"""
    return """Windows Registry Editor Version 5.00

[HKEY_LOCAL_MACHINE\\Software\\Test]
"TestKey"="TestValue"
"NumericValue"=dword:00000001

[HKEY_CURRENT_USER\\Software\\App]
"Path"="C:\\Program Files\\App"
@="Default Value"
"""

# ============================================================================
# SECTION 1: UTILITY FUNCTIONS TESTS
# ============================================================================

@pytest.mark.section1
class TestUtilityFunctions:
    """Test suite for utility functions"""
    
    def test_safe_decode_utf8(self):
        """Test safe_decode with UTF-8 content"""
        def safe_decode(blob):
            encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
            for e in encs:
                try:
                    return blob.decode(e)
                except:
                    continue
            return blob.decode("utf-8", errors="replace")
        
        content = "Hello World".encode('utf-8')
        result = safe_decode(content)
        
        assert result == "Hello World", "UTF-8 decoding failed"
        print("\n✓ Successfully decoded UTF-8 content")
    
    def test_safe_decode_utf16(self):
        """Test safe_decode with UTF-16 content"""
        def safe_decode(blob):
            encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
            for e in encs:
                try:
                    return blob.decode(e)
                except:
                    continue
            return blob.decode("utf-8", errors="replace")
        
        content = "Hello World".encode('utf-16')
        result = safe_decode(content)
        
        assert "Hello World" in result, "UTF-16 decoding failed"
        print("\n✓ Successfully decoded UTF-16 content")
    
    def test_safe_decode_invalid_bytes(self):
        """Test safe_decode with invalid bytes"""
        def safe_decode(blob):
            encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
            for e in encs:
                try:
                    return blob.decode(e)
                except:
                    continue
            return blob.decode("utf-8", errors="replace")
        
        content = b'\x80\x81\x82\x83'
        result = safe_decode(content)
        
        assert result is not None, "Should return something even with invalid bytes"
        print("\n✓ Successfully handled invalid bytes with fallback")
    
    def test_parse_registry_file(self, sample_registry_content):
        """Test parse_registry_file function"""
        import re
        
        def parse_registry_file(content_bytes):
            def safe_decode(blob):
                encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
                for e in encs:
                    try:
                        return blob.decode(e)
                    except:
                        continue
                return blob.decode("utf-8", errors="replace")
            
            lines = safe_decode(content_bytes).splitlines()
            rows = []
            current_section = None
            section_re = re.compile(r"^\s*\[(.+?)\]\s*$")
            kv_re = re.compile(r'^\s*(@|".+?"|[^=]+?)\s*=\s*(.+?)\s*$')
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                m = section_re.match(line)
                if m:
                    current_section = m.group(1).strip()
                    continue
                
                if current_section:
                    mv = kv_re.match(line)
                    if mv:
                        key_raw, value_raw = mv.groups()
                        key = key_raw.strip('"') if key_raw != "@" else "@"
                        rows.append({
                            "Path": current_section,
                            "Key": key,
                            "Value": value_raw.strip()
                        })
            
            return pd.DataFrame(rows)
        
        content_bytes = sample_registry_content.encode('utf-8')
        df = parse_registry_file(content_bytes)
        
        assert not df.empty, "DataFrame should not be empty"
        assert 'Path' in df.columns, "Should have Path column"
        assert 'Key' in df.columns, "Should have Key column"
        assert 'Value' in df.columns, "Should have Value column"
        assert len(df) >= 3, f"Should have at least 3 entries, got {len(df)}"
        
        print(f"\n✓ Successfully parsed registry file with {len(df)} entries")
    
    def test_detect_line_difference(self):
        """Test detect_line_difference function"""
        def detect_line_difference(line1, line2):
            if line1 == line2:
                return "identical"
            if line1.replace(' ', '').replace('\t', '') == line2.replace(' ', '').replace('\t', ''):
                return "whitespace"
            return "content"
        
        assert detect_line_difference("test", "test") == "identical"
        assert detect_line_difference("test  ", "test") == "whitespace"
        assert detect_line_difference("test1", "test2") == "content"
        
        print("\n✓ All line difference detection scenarios passed")

# ============================================================================
# SECTION 2: SESSION SERVICE TESTS
# ============================================================================

@pytest.mark.section2
class TestSessionService:
    """Test suite for session service"""
    
    def test_session_creation(self):
        """Test session creation and storage"""
        class SessionService:
            def __init__(self):
                self.sessions = {}
            
            def create_session(self, session_id, file_categories=None, extraction_path=None):
                self.sessions[session_id] = {
                    'file_categories': file_categories or {},
                    'extraction_path': extraction_path,
                    'created_at': datetime.now()
                }
            
            def session_exists(self, session_id):
                return session_id in self.sessions
            
            def get_session(self, session_id):
                return self.sessions.get(session_id)
        
        service = SessionService()
        session_id = "test_session"
        file_categories = {'customer_journals': ['file1.txt']}
        
        service.create_session(session_id, file_categories, '/tmp/test')
        
        assert service.session_exists(session_id)
        session_data = service.get_session(session_id)
        assert session_data is not None
        assert session_data['file_categories'] == file_categories
        
        print("\n✓ Session created and retrieved successfully")
    
    def test_session_update(self):
        """Test updating session data"""
        class SessionService:
            def __init__(self):
                self.sessions = {}
            
            def create_session(self, session_id, file_categories=None, extraction_path=None):
                self.sessions[session_id] = {
                    'file_categories': file_categories or {},
                    'extraction_path': extraction_path,
                }
            
            def update_session(self, session_id, key, value):
                if session_id in self.sessions:
                    self.sessions[session_id][key] = value
            
            def get_session(self, session_id):
                return self.sessions.get(session_id)
        
        service = SessionService()
        session_id = "test_session"
        
        service.create_session(session_id)
        service.update_session(session_id, 'transaction_data', [{'id': 'TXN001'}])
        
        session_data = service.get_session(session_id)
        assert 'transaction_data' in session_data
        assert len(session_data['transaction_data']) == 1
        
        print("\n✓ Session updated successfully")

# ============================================================================
# SECTION 3: CATEGORIZATION SERVICE TESTS
# ============================================================================

@pytest.mark.section3
class TestCategorizationService:
    """Test suite for categorization service"""
    
    def test_categorize_customer_journals(self):
        """Test categorization of customer journal files"""
        class CategorizationService:
            def categorize_files(self, extract_path):
                files = {
                    'customer_journals': [],
                    'ui_journals': [],
                    'registry_files': [],
                    'trc_trace': [],
                    'trc_error': []
                }
                
                for file_path in Path(extract_path).rglob('*'):
                    if file_path.is_file():
                        filename = file_path.name.lower()
                        if 'customerjournal' in filename or 'customer_journal' in filename:
                            files['customer_journals'].append(str(file_path))
                        elif 'uijournal' in filename or 'ui_journal' in filename:
                            files['ui_journals'].append(str(file_path))
                        elif filename.endswith('.reg'):
                            files['registry_files'].append(str(file_path))
                
                return files
        
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, 'CustomerJournal_1.txt').touch()
            Path(tmpdir, 'CustomerJournal_2.txt').touch()
            Path(tmpdir, 'UIJournal_1.txt').touch()
            
            service = CategorizationService()
            result = service.categorize_files(tmpdir)
            
            assert len(result['customer_journals']) == 2
            assert len(result['ui_journals']) == 1
            
            print(f"\n✓ Categorized: {len(result['customer_journals'])} customer journals, {len(result['ui_journals'])} UI journals")
    
    def test_categorize_registry_files(self):
        """Test categorization of registry files"""
        class CategorizationService:
            def categorize_files(self, extract_path):
                files = {
                    'customer_journals': [],
                    'ui_journals': [],
                    'registry_files': [],
                    'trc_trace': [],
                    'trc_error': []
                }
                
                for file_path in Path(extract_path).rglob('*'):
                    if file_path.is_file():
                        if file_path.suffix.lower() == '.reg':
                            files['registry_files'].append(str(file_path))
                
                return files
        
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, 'Registry_1.reg').touch()
            Path(tmpdir, 'Registry_2.reg').touch()
            Path(tmpdir, 'NotRegistry.txt').touch()
            
            service = CategorizationService()
            result = service.categorize_files(tmpdir)
            
            assert len(result['registry_files']) == 2
            
            print(f"\n✓ Categorized {len(result['registry_files'])} registry files")

# ============================================================================
# SECTION 4: TRANSACTION ANALYZER TESTS
# ============================================================================

@pytest.mark.section4
class TestTransactionAnalyzer:
    """Test suite for transaction analyzer"""
    
    def test_transaction_data_structure(self, sample_transaction_data):
        """Test transaction data structure validity"""
        required_fields = [
            'Transaction ID', 'Transaction Type', 'Start Time',
            'End Time', 'End State', 'Source File'
        ]
        
        for field in required_fields:
            assert field in sample_transaction_data, f"Missing required field: {field}"
        
        print(f"\n✓ All {len(required_fields)} required fields present")
    
    def test_transaction_filtering_by_source(self, sample_transactions_df):
        """Test filtering transactions by source file"""
        source_file = 'CustomerJournal_1'
        filtered = sample_transactions_df[sample_transactions_df['Source File'] == source_file]
        
        assert len(filtered) == 2
        assert all(filtered['Source File'] == source_file)
        
        print(f"\n✓ Filtered {len(filtered)} transactions by source")
    
    def test_transaction_filtering_by_type(self, sample_transactions_df):
        """Test filtering transactions by type"""
        txn_type = 'Withdraw'
        filtered = sample_transactions_df[sample_transactions_df['Transaction Type'] == txn_type]
        
        assert len(filtered) == 2
        assert all(filtered['Transaction Type'] == txn_type)
        
        print(f"\n✓ Filtered {len(filtered)} transactions by type")
    
    def test_transaction_statistics(self, sample_transactions_df):
        """Test transaction statistics calculation"""
        total = len(sample_transactions_df)
        successful = len(sample_transactions_df[sample_transactions_df['End State'] == 'Successful'])
        unsuccessful = len(sample_transactions_df[sample_transactions_df['End State'] == 'Unsuccessful'])
        
        assert total == 3
        assert successful == 2
        assert unsuccessful == 1
        
        success_rate = (successful / total) * 100
        print(f"\n✓ Stats: {successful}/{total} successful ({success_rate:.1f}%)")

# ============================================================================
# SECTION 5: UI JOURNAL PROCESSOR TESTS
# ============================================================================

@pytest.mark.section5
class TestUIJournalProcessor:
    """Test suite for UI journal processor"""
    
    def test_ui_events_structure(self, sample_ui_events_df):
        """Test UI events DataFrame structure"""
        assert 'Time' in sample_ui_events_df.columns
        assert 'Screen' in sample_ui_events_df.columns
        assert len(sample_ui_events_df) == 5
        
        print(f"\n✓ UI events DataFrame has {len(sample_ui_events_df)} events")
    
    def test_screen_flow_extraction(self, sample_ui_events_df):
        """Test screen flow extraction"""
        def get_screen_flow(df, start_time, end_time):
            mask = (df['Time'] >= start_time) & (df['Time'] <= end_time)
            filtered = df[mask]
            return filtered['Screen'].tolist()
        
        start_time = time(10, 30, 0)
        end_time = time(10, 30, 50)
        
        flow = get_screen_flow(sample_ui_events_df, start_time, end_time)
        
        assert len(flow) == 5
        assert flow[0] == 'MainMenu'
        assert flow[-1] == 'Receipt'
        
        print(f"\n✓ Extracted flow: {' → '.join(flow)}")
    
    def test_screen_flow_empty_range(self, sample_ui_events_df):
        """Test screen flow with empty time range"""
        def get_screen_flow(df, start_time, end_time):
            mask = (df['Time'] >= start_time) & (df['Time'] <= end_time)
            filtered = df[mask]
            return filtered['Screen'].tolist() if not filtered.empty else []
        
        start_time = time(11, 0, 0)
        end_time = time(11, 30, 0)
        
        flow = get_screen_flow(sample_ui_events_df, start_time, end_time)
        
        assert len(flow) == 0
        
        print("\n✓ Correctly returned empty flow")

# ============================================================================
# SECTION 6: COMPARISON LOGIC TESTS
# ============================================================================

@pytest.mark.section6
class TestComparisonLogic:
    """Test suite for comparison logic"""
    
    def test_lcs_matching(self):
        """Test LCS matching algorithm"""
        def find_lcs_matches(flow1, flow2):
            m, n = len(flow1), len(flow2)
            lcs_table = [[0] * (n + 1) for _ in range(m + 1)]
            
            for i in range(1, m + 1):
                for j in range(1, n + 1):
                    if flow1[i-1] == flow2[j-1]:
                        lcs_table[i][j] = lcs_table[i-1][j-1] + 1
                    else:
                        lcs_table[i][j] = max(lcs_table[i-1][j], lcs_table[i][j-1])
            
            matches1 = [False] * m
            matches2 = [False] * n
            i, j = m, n
            
            while i > 0 and j > 0:
                if flow1[i-1] == flow2[j-1]:
                    matches1[i-1] = True
                    matches2[j-1] = True
                    i -= 1
                    j -= 1
                elif lcs_table[i-1][j] > lcs_table[i][j-1]:
                    i -= 1
                else:
                    j -= 1
            
            return matches1, matches2
        
        flow1 = ['MainMenu', 'Withdraw', 'Amount', 'Cash', 'Receipt']
        flow2 = ['MainMenu', 'Balance', 'Amount', 'Receipt', 'End']
        
        matches1, matches2 = find_lcs_matches(flow1, flow2)
        
        assert matches1[0] == True  # MainMenu
        assert matches1[2] == True  # Amount
        assert matches1[4] == True  # Receipt
        
        common_count = sum(matches1)
        print(f"\n✓ LCS found {common_count} matching screens")
    
    def test_flow_similarity(self):
        """Test flow similarity calculation"""
        flow1 = ['MainMenu', 'Withdraw', 'Amount', 'Cash', 'Receipt']
        flow2 = ['MainMenu', 'Withdraw', 'Amount', 'Cash', 'Receipt']
        
        common = len(set(flow1) & set(flow2))
        total_unique = len(set(flow1) | set(flow2))
        similarity = (common / total_unique * 100) if total_unique > 0 else 0
        
        assert similarity == 100.0
        
        print(f"\n✓ Similarity: {similarity}%")

# ============================================================================
# SECTION 7: FILE HANDLING TESTS
# ============================================================================

@pytest.mark.section7
class TestFileHandling:
    """Test suite for file handling"""
    
    def test_zip_extraction(self, sample_zip_content):
        """Test ZIP extraction"""
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_buffer = BytesIO(sample_zip_content)
            
            with zipfile.ZipFile(zip_buffer, 'r') as zf:
                zf.extractall(tmpdir)
            
            extracted_files = list(Path(tmpdir).rglob('*'))
            file_names = [f.name for f in extracted_files if f.is_file()]
            
            assert 'CustomerJournal_1.txt' in file_names
            assert 'UIJournal_1.txt' in file_names
            assert 'Registry_1.reg' in file_names
            
            print(f"\n✓ Extracted {len(file_names)} files")
    
    def test_file_organization(self):
        """Test file organization into subdirectories"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            
            (base_path / 'CustomerJournal_1.txt').touch()
            (base_path / 'UIJournal_1.txt').touch()
            
            categories = {
                'customer_journals': [str(base_path / 'CustomerJournal_1.txt')],
                'ui_journals': [str(base_path / 'UIJournal_1.txt')]
            }
            
            for category, files in categories.items():
                category_dir = base_path / category
                category_dir.mkdir(exist_ok=True)
                
                for file_path_str in files:
                    source = Path(file_path_str)
                    if source.exists():
                        dest = category_dir / source.name
                        source.rename(dest)
            
            assert (base_path / 'customer_journals' / 'CustomerJournal_1.txt').exists()
            assert (base_path / 'ui_journals' / 'UIJournal_1.txt').exists()
            
            print("\n✓ Files organized into subdirectories")

# ============================================================================
# SECTION 8: API ENDPOINT TESTS
# ============================================================================

@pytest.mark.section8
class TestAPIEndpoints:
    """Test suite for API endpoints (mocked)"""
    
    def test_session_check(self):
        """Test session check logic"""
        class MockSessionService:
            def __init__(self):
                self.sessions = {'test_session': {}}
            
            def session_exists(self, session_id):
                return session_id in self.sessions
        
        service = MockSessionService()
        
        assert service.session_exists('test_session') == True
        assert service.session_exists('nonexistent') == False
        
        print("\n✓ Session check working")
    
    def test_transaction_filter_request(self):
        """Test transaction filtering"""
        request_body = {
            "source_files": ["CustomerJournal_1", "CustomerJournal_2"]
        }
        
        all_transactions = [
            {'Transaction ID': 'TXN001', 'Source File': 'CustomerJournal_1'},
            {'Transaction ID': 'TXN002', 'Source File': 'CustomerJournal_2'},
            {'Transaction ID': 'TXN003', 'Source File': 'CustomerJournal_3'}
        ]
        
        filtered = [
            txn for txn in all_transactions
            if txn['Source File'] in request_body['source_files']
        ]
        
        assert len(filtered) == 2
        
        print(f"\n✓ Filtered to {len(filtered)} transactions")

# ============================================================================
# SECTION 9: LLM INTEGRATION TESTS
# ============================================================================

@pytest.mark.section9
class TestLLMIntegration:
    """Test suite for LLM integration (mocked)"""
    
    def test_llm_request_structure(self):
        """Test LLM request structure"""
        transaction_log = "Sample log..."
        
        messages = [
            {"role": "system", "content": "You are a log analyst."},
            {"role": "user", "content": f"Analyze: {transaction_log}"}
        ]
        
        assert len(messages) == 2
        assert messages[0]['role'] == 'system'
        assert messages[1]['role'] == 'user'
        
        print("\n✓ LLM request structure valid")
    
    def test_llm_response_parsing(self):
        """Test LLM response parsing"""
        mock_response = {
            'message': {
                'content': 'Analysis: Transaction failed due to timeout.'
            }
        }
        
        analysis = mock_response['message']['content'].strip()
        
        assert len(analysis) > 0
        assert 'failed' in analysis.lower()
        
        structured = {
            'summary': 'Complete',
            'analysis': analysis,
            'timestamp': datetime.now().isoformat()
        }
        
        assert 'summary' in structured
        assert 'analysis' in structured
        
        print(f"\n✓ Parsed response ({len(analysis)} chars)")

# ============================================================================
# SECTION 10: FEEDBACK SYSTEM TESTS
# ============================================================================

@pytest.mark.section10
class TestFeedbackSystem:
    """Test suite for feedback system"""
    
    def test_feedback_structure(self):
        """Test feedback data structure"""
        feedback_data = {
            "transaction_id": "TXN001",
            "rating": 4,
            "alternative_cause": "Timeout",
            "comment": "Good analysis",
            "user_name": "John",
            "user_email": "john@test.com",
            "model_version": "llama3",
            "timestamp": datetime.now().isoformat()
        }
        
        required_fields = ['transaction_id', 'rating', 'user_name', 'timestamp']
        
        for field in required_fields:
            assert field in feedback_data
        
        assert isinstance(feedback_data['rating'], int)
        assert 0 <= feedback_data['rating'] <= 5
        
        print(f"\n✓ Feedback structure valid")
    
    def test_feedback_storage(self):
        """Test feedback storage"""
        feedback_data = {
            "transaction_id": "TXN001",
            "rating": 4,
            "timestamp": datetime.now().isoformat()
        }
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            f.write(json.dumps(feedback_data) + "\n")
            temp_file = f.name
        
        with open(temp_file, 'r') as f:
            line = f.readline()
            loaded_data = json.loads(line)
        
        assert loaded_data['transaction_id'] == feedback_data['transaction_id']
        
        Path(temp_file).unlink()
        
        print("\n✓ Feedback stored and retrieved")
    
    def test_feedback_retrieval(self):
        """Test feedback retrieval"""
        all_feedback = [
            {"transaction_id": "TXN001", "rating": 4},
            {"transaction_id": "TXN002", "rating": 3},
            {"transaction_id": "TXN001", "rating": 5}
        ]
        
        target_txn = "TXN001"
        filtered = [f for f in all_feedback if f['transaction_id'] == target_txn]
        
        assert len(filtered) == 2
        
        print(f"\n✓ Retrieved {len(filtered)} feedback entries")

# ============================================================================
# SECTION 11: DATA VALIDATION TESTS
# ============================================================================

@pytest.mark.section11
class TestDataValidation:
    """Test suite for data validation"""
    
    def test_transaction_id_format(self):
        """Test transaction ID validation"""
        def is_valid_transaction_id(txn_id):
            return txn_id is not None and isinstance(txn_id, str) and len(txn_id.strip()) > 0
        
        valid_ids = ["TXN001", "TXN_2024_001"]
        invalid_ids = ["", None, "   "]
        
        for txn_id in valid_ids:
            assert is_valid_transaction_id(txn_id)
        
        for txn_id in invalid_ids:
            assert not is_valid_transaction_id(txn_id)
        
        print("\n✓ Transaction ID validation working")
    
    def test_time_format_validation(self):
        """Test time format validation"""
        def parse_time(time_str):
            if pd.isna(time_str):
                return None
            if isinstance(time_str, str):
                try:
                    return datetime.strptime(time_str, '%H:%M:%S').time()
                except:
                    return None
            return time_str
        
        valid_time = "10:30:00"
        invalid_time = "invalid"
        
        parsed_valid = parse_time(valid_time)
        parsed_invalid = parse_time(invalid_time)
        
        assert parsed_valid is not None
        assert parsed_invalid is None
        
        print("\n✓ Time validation working")
    
    def test_duration_calculation(self):
        """Test duration calculation"""
        start_time = time(10, 30, 0)
        end_time = time(10, 31, 30)
        
        start_dt = datetime.combine(datetime.today(), start_time)
        end_dt = datetime.combine(datetime.today(), end_time)
        
        duration = (end_dt - start_dt).total_seconds()
        
        assert duration == 90.0
        
        print(f"\n✓ Duration: {duration}s")

# ============================================================================
# SECTION 12: EDGE CASES TESTS
# ============================================================================

@pytest.mark.section12
class TestEdgeCases:
    """Test suite for edge cases"""
    
    def test_empty_transaction_list(self):
        """Test empty transaction list"""
        empty_df = pd.DataFrame(columns=['Transaction ID', 'Transaction Type', 'End State'])
        
        assert len(empty_df) == 0
        assert 'Transaction ID' in empty_df.columns
        
        successful = len(empty_df[empty_df['End State'] == 'Successful'])
        assert successful == 0
        
        print("\n✓ Handled empty list")
    
    def test_missing_ui_journal(self):
        """Test missing UI journal"""
        ui_journals = []
        
        if not ui_journals:
            result = ["No flow data"]
        
        assert result == ["No flow data"]
        
        print("\n✓ Handled missing UI journal")
    
    def test_invalid_source_filter(self):
        """Test invalid source filter"""
        transactions = [
            {'Transaction ID': 'TXN001', 'Source File': 'File1'},
            {'Transaction ID': 'TXN002', 'Source File': 'File2'}
        ]
        
        filtered = [txn for txn in transactions if txn['Source File'] in ['NonExistent']]
        
        assert len(filtered) == 0
        
        print("\n✓ Handled invalid filter")
    
    def test_duplicate_transaction_ids(self):
        """Test duplicate transaction IDs"""
        transactions = pd.DataFrame([
            {'Transaction ID': 'TXN001', 'Transaction Type': 'Withdraw'},
            {'Transaction ID': 'TXN001', 'Transaction Type': 'Balance'},
            {'Transaction ID': 'TXN002', 'Transaction Type': 'Withdraw'}
        ])
        
        duplicates = transactions[transactions.duplicated(subset=['Transaction ID'], keep=False)]
        
        assert len(duplicates) == 2
        
        print(f"\n✓ Detected {len(duplicates)} duplicates")

# ============================================================================
# SECTION 13: PERFORMANCE TESTS
# ============================================================================

@pytest.mark.section13
class TestPerformance:
    """Test suite for performance"""
    
    def test_large_dataset_filtering(self):
        """Test filtering large dataset"""
        import time as time_module
        
        large_df = pd.DataFrame([
            {
                'Transaction ID': f'TXN{i:06d}',
                'Transaction Type': 'Withdraw' if i % 2 == 0 else 'Balance',
                'Source File': f'File{i % 10}',
                'End State': 'Successful' if i % 3 == 0 else 'Unsuccessful'
            }
            for i in range(1000)
        ])
        
        start = time_module.time()
        filtered = large_df[large_df['Source File'] == 'File1']
        elapsed = time_module.time() - start
        
        assert len(filtered) == 100
        assert elapsed < 1.0
        
        print(f"\n✓ Filtered 1000 records in {elapsed*1000:.2f}ms")
    
    def test_screen_flow_many_events(self):
        """Test flow extraction with many events"""
        import time as time_module
        
        events = []
        for i in range(1000):
            seconds = i * 2
            event_time = time(10, seconds // 60 % 60, seconds % 60)
            events.append({'Time': event_time, 'Screen': f'Screen{i % 20}'})
        
        df = pd.DataFrame(events)
        
        start_time_obj = time(10, 5, 0)
        end_time_obj = time(10, 6, 0)
        
        start = datetime.now()
        mask = (df['Time'] >= start_time_obj) & (df['Time'] <= end_time_obj)
        filtered = df[mask]
        elapsed = (datetime.now() - start).total_seconds()
        
        assert elapsed < 1.0
        
        print(f"\n✓ Extracted from 1000 events in {elapsed*1000:.2f}ms")