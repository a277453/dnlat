 DN Diagnostics Analysis Platform

The **DN Diagnostics Analysis Platform** is a powerful web application built with FastAPI and Streamlit, designed to ingest, parse, and analyze complex diagnostic log packages from Diebold Nixdorf (DN) systems. It provides a comprehensive suite of tools for support engineers and developers to quickly diagnose issues by untangling nested archives, categorizing log files, and visualizing transaction flows.

## Key Features

*   **Robust ZIP Processing**: Intelligently extracts files from complex, deeply nested ZIP archives, automatically handling non-standard formats and pathing issues that fail standard libraries.
*   **Automated File Categorization**: Employs a sophisticated, content-based detection engine to accurately identify and categorize dozens of log types, including Customer Journals, UI Journals, TRC Traces, and Registry Files.
*   **In-Memory ACU Parsing**: Utilizes a high-performance, low-level parser to extract ACU (Agilis Configuration Unit) files and their associated XSD documentation directly from the ZIP byte stream.
*   **Advanced Transaction Analysis**: Provides endpoints for parsing customer journals, visualizing individual and consolidated transaction flows, and comparing the UI paths of different transactions.
*   **Interactive Frontend**: A comprehensive Streamlit UI allows for easy file uploads, data visualization, and interaction with the backend analysis tools.
*   **Unit Tested**: Core components like the ACU and Registry parsers are validated with unit tests to ensure reliability and correctness.

## Tech Stack

*   **Backend**: FastAPI, Uvicorn
*   **Frontend**: Streamlit
*   **Data Processing**: Pandas, lxml
*   **Visualization**: Plotly
*   **Testing**: unittest

## How It Works

The platform follows a multi-stage pipeline to process diagnostic packages:

1.  **Upload**: A user uploads a single, potentially complex `.zip` archive via the Streamlit UI.
2.  **In-Memory Extraction (ACU)**: A specialized, low-level parser scans the ZIP file's byte stream to find and extract all ACU-related XML and XSD files without writing to disk.
3.  **Full Extraction**: A robust extraction service unpacks the entire ZIP archive, including all nested ZIPs, to a temporary directory on disk. It intelligently filters out irrelevant "junk" files to optimize performance.
4.  **Categorization**: The system scans the extracted files and uses a combination of filename patterns and deep content analysis to sort each file into its correct category (e.g., `customer_journals`, `ui_journals`).
5.  **Analysis**: With all files categorized, the platform's analysis tools become available, allowing the user to parse transaction data, visualize UI flows, compare registry files, and more.

## Project Structure

```
├── api/
│   └── routes.py             # FastAPI endpoints and main API logic
├── config/
│   └── dnLogAtConfig.xml     # Configuration for transaction parsing
├── modules/
│   ├── categorization.py     # File categorization service
│   ├── configManager.py      # XML config and file type detection logic
│   ├── extraction.py         # ZIP extraction and ACU parsing services
│   ├── processing.py         # Prepares final API responses
│   ├── schemas.py            # Pydantic models for API requests/responses
│   └── ...                   # Other business logic modules
├── tests/
│   ├── test_acu_parser.py    # Unit tests for the ACU parser
│   └── test_registry_parser.py # Unit tests for the Registry parser
├── main.py                     # FastAPI application entry point
├── streamlit_app.py            # Streamlit UI application entry point
├── requirements.txt            # Project dependencies
└── README.md                   # This file
```

## Setup and Installation

### Prerequisites
*   Python 3.8+
*   `pip` and `venv`

### Installation Steps

1.  **Clone the repository:**
    ```bash
git clone https://github.com/sohamparabbb/DNLoggingTool.git
cd DNLoggingTool
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    # On Windows
    python -m venv venv
    venv\Scripts\activate
    
    # On macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

The application consists of a FastAPI backend and a Streamlit frontend. They must be run in two separate terminals.

1.  **Start the FastAPI Backend:**
    Open a terminal in the project root and run:
    ```bash
    python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
    ```

2.  **Start the Streamlit Frontend:**
    Open a second terminal in the project root and run:
    ```bash
    python -m streamlit run streamlit_app.py --server.port 8501
    ```
    You can now access the web interface in your browser at `http://localhost:8501`.

## Running Tests

This project uses Python's built-in unittest framework. To run all tests for core components, execute the following command from the project's root directory:

```bash
python -m unittest discover tests
```
