# DN Diagnostics Analysis Platform - AI Coding Instructions

## Architecture Overview

This is a **two-tier web application** analyzing Diebold Nixdorf (DN) diagnostic log packages:
- **Backend**: FastAPI (`main.py`) running on localhost:8000 with REST endpoints
- **Frontend**: Streamlit (`streamlit_app.py`) on localhost:8501 for file upload and visualization

### Core Data Flow
1. User uploads `.zip` file via Streamlit
2. FastAPI extracts & categorizes files in memory and to disk
3. Specialized parsers extract ACU configs, transaction data, registry files
4. Analysis endpoints generate visualizations and comparisons
5. Results streamed back to Streamlit UI

### Key Modules & Responsibilities

| Module | Purpose |
|--------|---------|
| [extraction.py](modules/extraction.py) | Low-level ZIP parsing, nested ZIP recursion, ACU byte-stream extraction |
| [categorization.py](modules/categorization.py) | Content-based file type detection (customer journals, traces, registry files) |
| [configManager.py](modules/configManager.py) | Parses [dnLogAtConfig.xml](config/dnLogAtConfig.xml) for transaction metadata & parsing rules |
| [transaction_analyzer.py](modules/transaction_analyzer.py) | Builds transaction flow graphs and UI path comparisons |
| [xml_parser_logic.py](modules/xml_parser_logic.py) | Low-level XML/ACU parsing with XSD validation |
| [schemas.py](modules/schemas.py) | Pydantic models for all API request/response contracts |
| [api/routes.py](api/routes.py) | FastAPI endpoints orchestrating extraction → categorization → analysis pipeline |

## Critical Developer Workflows

### Starting Development
```bash
# Terminal 1: Backend
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload

# Terminal 2: Frontend  
python -m streamlit run streamlit_app.py --server.port 8501
```
**Never run in production with `--reload`**. Backend auto-reloads on code changes during dev.

### Testing Core Parsers
```bash
# Unit tests for parsers (validates correctness)
python -m unittest modules.test_acu_parser
python -m unittest modules.test_registry_parser
```

### Debugging File Processing
- **Extraction logs**: Check `logger` output for ZIP structure issues (nested ZIPs, junk files)
- **Categorization logic**: [categorization.py](modules/categorization.py) L45-120 shows filename patterns for detection
- **Configuration parsing**: [configManager.py](modules/configManager.py) L5-60 parses transaction start/end markers from XML

## Project-Specific Conventions & Patterns

### Logging Pattern
All modules use centralized logger:
```python
from modules.logging_config import logger
logger.info("Message")  # Always use - don't use print()
```
**Frontend uses separate logger**: `from modules.streamlit_logger import logger as frontend_logger`

### API Design Pattern
All endpoints follow this structure:
```python
@router.post("/analyze-package")
async def analyze_package(file: UploadFile = File(...)):
    """Docstring: FUNCTION/CLASS (uppercase), DESCRIPTION, USAGE, PARAMETERS, RETURNS, RAISES"""
    service = ZipExtractionService()
    result = service.extract_and_categorize(...)
    return FileCategorizationResponse(...)  # Always use Pydantic schemas
```
- All file processing uses **temporary directories** (never persist user data)
- Responses must use Pydantic models defined in [schemas.py](modules/schemas.py)

### File Categorization Pattern
Three-stage detection in [categorization.py](modules/categorization.py):
1. Filename pattern matching (fast)
2. Directory hierarchy analysis
3. Content inspection (ACU headers, XML markers, registry format)

**Key insight**: ACU files are extracted separately via `extract_from_zip_bytes()` and merged into categorization results by the API endpoint.

### Configuration Management
- Transaction types, start/end markers, field mappings: **all in [dnLogAtConfig.xml](config/dnLogAtConfig.xml)**
- Load once at startup via [configManager.py](modules/configManager.py) `xml_to_dict()`
- **Never hardcode transaction logic** - always reference config

## Integration Points & External Dependencies

### Major Dependencies
- **Parsing**: `lxml`, `xmltodict`, `xmldict` (XML/ACU configuration files)
- **Data**: `pandas` (DataFrames for analysis), `plotly` (interactive visualizations)
- **Backend**: `fastapi`, `uvicorn`, `pydantic` (API contracts)
- **Frontend**: `streamlit`, `requests` (calls FastAPI backend)
- **DB**: `psycopg2-binary` (PostgreSQL support for future auth)

### Cross-Component Communication Pattern
1. **Streamlit → FastAPI**: POST file to `/upload` endpoint as `UploadFile`
2. **FastAPI response**: `FileCategorizationResponse` Pydantic model with categorized file paths
3. **Streamlit processes**: Response paths, displays categorized files, allows drill-down analysis
4. **Analysis endpoints**: `/analyze-transactions`, `/compare-ui-paths` take file paths + metadata

### ACU Extraction (Special Case)
- **Why separate**: ACU files are embedded in nested ZIPs as byte streams; need low-level parsing to avoid disk writes
- **Implementation**: [extraction.py](modules/extraction.py) `extract_from_zip_bytes()` scans ZIP central directory
- **Integration**: Results merged into categorization response under `acu_files` category

## Common Pitfalls & Best Practices

❌ **Avoid**:
- Using `print()` instead of logger
- Hardcoding file paths (use temp directories, respect OS path separators)
- Processing large uploads synchronously (use async/await)
- Skipping Pydantic validation (always define response schemas)
- Duplicate categorization logic (centralize in [categorization.py](modules/categorization.py))

✅ **Do**:
- Use `Path` from pathlib for cross-platform compatibility
- Define all new API responses in [schemas.py](modules/schemas.py)
- Log at appropriate levels: `info` for flow, `debug` for details, `error` for issues
- Test parsers with real DN diagnostic files (see `test_*.json` outputs)
- Reference [dnLogAtConfig.xml](config/dnLogAtConfig.xml) for transaction rules, not hardcoding

## File Patterns & Detection Rules

**Recognized file categories** (from [categorization.py](modules/categorization.py) L33-42):
- `customer_journals`: Files with "customerjournal", "customer_journal", ".jrn"
- `ui_journals`: Files with "uijournal", "ui_journal"
- `trc_trace`: Files with ".trc", "trace"
- `registry_files`: Files with ".reg", "reg.txt", "registry"
- `acu_files`: ACU XML configs extracted from ZIP byte stream
- `unidentified`: Files that don't match above patterns

## Environment & Setup

- **Python**: 3.8+ (tested; 3.9+ recommended)
- **Virtual env**: Always use `venv` (see [RUNNING_CODE.txt](RUNNING_CODE.txt) for setup)
- **Config file**: [dnLogAtConfig.xml](config/dnLogAtConfig.xml) must exist (transaction parsing depends on it)
- **Temp storage**: `%TEMP%\dn_extracts` on Windows; `/tmp/dn_extracts` on Unix
