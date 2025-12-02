import streamlit as st
import requests
import json
import pandas as pd
from pathlib import Path
import io
import zipfile
import re
import plotly.express as px

def create_comparison_flow_plotly(txn1_id, txn1_state, txn1_flow_screens, txn1_matches,
                                   txn2_id, txn2_state, txn2_flow_screens, txn2_matches):
    """Create a side-by-side Plotly visualization for transaction flow comparison"""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    # Create subplots: 1 row, 2 columns
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"Transaction 1: {txn1_id} ({txn1_state})", 
                       f"Transaction 2: {txn2_id} ({txn2_state})"),
        horizontal_spacing=0.1,
        specs=[[{"type": "xy"}, {"type": "xy"}]]
    )
    
    # Colors
    match_color = '#4A90E2'      # Blue for matches
    no_match_color = '#F5A623'   # Orange for non-matches
    
    max_screens = max(len(txn1_flow_screens), len(txn2_flow_screens))
    
    # Transaction 1 (left side)
    for i, (screen, is_match) in enumerate(zip(txn1_flow_screens, txn1_matches)):
        color = match_color if is_match else no_match_color
        y_pos = max_screens - 1 - i
        
        # Add box
        fig.add_shape(
            type="rect",
            x0=0.1, x1=0.9, y0=y_pos, y1=y_pos + 0.7,
            fillcolor=color,
            line=dict(color=color, width=2),
            row=1, col=1
        )
        
        # Add text
        fig.add_annotation(
            x=0.5, y=y_pos + 0.35,
            text=f"{i+1}. {screen}",
            showarrow=False,
            font=dict(color="white", size=10),
            xanchor="center", yanchor="middle",
            row=1, col=1
        )
    
    # Transaction 2 (right side)
    for i, (screen, is_match) in enumerate(zip(txn2_flow_screens, txn2_matches)):
        color = match_color if is_match else no_match_color
        y_pos = max_screens - 1 - i
        
        # Add box
        fig.add_shape(
            type="rect",
            x0=0.1, x1=0.9, y0=y_pos, y1=y_pos + 0.7,
            fillcolor=color,
            line=dict(color=color, width=2),
            row=1, col=2
        )
        
        # Add text
        fig.add_annotation(
            x=0.5, y=y_pos + 0.35,
            text=f"{i+1}. {screen}",
            showarrow=False,
            font=dict(color="white", size=10),
            xanchor="center", yanchor="middle",
            row=1, col=2
        )
    
    # Update layout
    height = max(600, max_screens * 100)
    
    fig.update_layout(
        height=height,
        showlegend=False,
        plot_bgcolor='#0E1117',
        paper_bgcolor='#0E1117',
        font=dict(color='white')
    )
    
    # Update axes
    for i in [1, 2]:
        fig.update_xaxes(
            showgrid=False, showticklabels=False, zeroline=False,
            range=[0, 1], row=1, col=i
        )
        fig.update_yaxes(
            showgrid=False, showticklabels=False, zeroline=False,
            range=[-0.5, max_screens], autorange=True, row=1, col=i
        )
    
    return fig

# Page configuration
st.set_page_config(
    page_title="DN Diagnostics Platform",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Professional CSS
st.markdown("""
    <style>
    /* Global Styles */
    .main {
        background-color: #0a0a0a;
        color: #e0e0e0;
    }
    
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }
    
    /* Sidebar Styles */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a1a 0%, #0f0f0f 100%);
        border-right: 1px solid #2a2a2a;
    }
    
    [data-testid="stSidebar"] h2 {
        color: #ffffff;
        font-weight: 600;
        font-size: 1.1rem;
        margin-bottom: 1.5rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Typography */
    h1 {
        color: #ffffff !important;
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        margin-bottom: 0.5rem !important;
        letter-spacing: -0.5px !important;
    }
    
    h2 {
        color: #ffffff !important;
        font-size: 1.75rem !important;
        font-weight: 600 !important;
        margin: 2rem 0 1rem 0 !important;
        border-bottom: 2px solid #2563eb;
        padding-bottom: 0.5rem;
    }
    
    h3 {
        color: #e0e0e0 !important;
        font-size: 1.25rem !important;
        font-weight: 600 !important;
        margin: 1.5rem 0 0.75rem 0 !important;
    }
    
    h4, h5, h6 {
        color: #c0c0c0 !important;
        font-weight: 500 !important;
    }
    
    p, span, div, label {
        color: #b0b0b0 !important;
        line-height: 1.6 !important;
    }
    
    /* Button Styles - Uniform and Professional */
    .stButton > button {
        background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
        color: #ffffff;
        border: none;
        padding: 0.75rem 2rem;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.95rem;
        letter-spacing: 0.3px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 4px 6px rgba(37, 99, 235, 0.2);
        width: 100%;
        height: 48px;
        text-transform: none;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #1d4ed8 0%, #1e40af 100%);
        box-shadow: 0 6px 12px rgba(37, 99, 235, 0.35);
        transform: translateY(-1px);
    }
    
    .stButton > button:active {
        transform: translateY(0);
        box-shadow: 0 2px 4px rgba(37, 99, 235, 0.3);
    }
    
    .stButton > button:disabled {
        background: linear-gradient(135deg, #374151 0%, #1f2937 100%);
        color: #6b7280;
        cursor: not-allowed;
        box-shadow: none;
    }
    
    /* File Uploader */
    [data-testid="stFileUploader"] {
        background-color: #1a1a1a;
        border: 2px dashed #404040;
        border-radius: 12px;
        padding: 2rem;
        transition: all 0.3s ease;
    }
    
    [data-testid="stFileUploader"]:hover {
        border-color: #2563eb;
        background-color: #1f1f1f;
    }
    
    /* Select Box - Uniform Style */
    .stSelectbox > div > div {
        background-color: #1a1a1a;
        border: 1px solid #404040;
        border-radius: 8px;
        color: #e0e0e0;
        height: 48px;
        transition: all 0.3s ease;
    }
    
    .stSelectbox > div > div:hover {
        border-color: #2563eb;
        background-color: #1f1f1f;
    }
    
    /* Text Input */
    .stTextInput > div > div > input {
        background-color: #1a1a1a;
        border: 1px solid #404040;
        border-radius: 8px;
        color: #e0e0e0;
        height: 48px;
        padding: 0 1rem;
        transition: all 0.3s ease;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #2563eb;
        box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.1);
    }
    
    /* Metric Cards */
    [data-testid="stMetricValue"] {
        font-size: 1.75rem;
        font-weight: 700;
        color: #ffffff;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.875rem;
        color: #9ca3af;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 500;
    }
    
    /* Data Tables */
    .dataframe {
        border: 1px solid #2a2a2a !important;
        border-radius: 8px;
        overflow: hidden;
    }
    
    .dataframe thead tr th {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.5px;
        padding: 1rem !important;
    }
    
    .dataframe tbody tr:hover {
        background-color: #1f1f1f !important;
    }
    
    /* Info/Warning/Success Boxes */
    .stAlert {
        border-radius: 8px;
        border-left-width: 4px;
        padding: 1rem 1.25rem;
    }
    
    /* Construction Badge */
    .construction-badge {
        display: inline-block;
        background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
        color: #000000;
        padding: 0.375rem 0.875rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Section Divider */
    hr {
        border: none;
        border-top: 1px solid #2a2a2a;
        margin: 2rem 0;
    }
    
    /* Diff Viewer Styles */
    .diff-viewer {
        display: flex;
        gap: 1rem;
        margin-top: 1.5rem;
    }
    
    .diff-pane {
        flex: 1;
        background-color: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 8px;
        padding: 1rem;
        overflow-x: auto;
        font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
        font-size: 0.875rem;
    }
    
    .diff-pane-header {
        background-color: #0f0f0f;
        padding: 0.75rem 1rem;
        border-bottom: 1px solid #2a2a2a;
        font-weight: 600;
        color: #ffffff;
        border-radius: 8px 8px 0 0;
        margin: -1rem -1rem 1rem -1rem;
    }
    
    .diff-line {
        padding: 0.25rem 0.5rem;
        white-space: pre-wrap;
        word-break: break-all;
        border-left: 3px solid transparent;
    }
    
    .diff-line-number {
        display: inline-block;
        width: 50px;
        color: #666666;
        text-align: right;
        margin-right: 1rem;
        user-select: none;
        font-weight: 500;
    }
    
    .diff-content-change {
        background-color: rgba(239, 68, 68, 0.15);
        border-left-color: #ef4444;
    }
    
    .diff-whitespace-change {
        background-color: rgba(168, 85, 247, 0.12);
        border-left-color: #a855f7;
    }
    
    .diff-identical {
        background-color: transparent;
    }
    
    /* Legend Items */
    .legend-item {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem 1rem;
        background-color: #1a1a1a;
        border-radius: 6px;
        font-size: 0.875rem;
        font-weight: 500;
    }
    
    .legend-color {
        width: 20px;
        height: 20px;
        border-radius: 4px;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
        background-color: #1a1a1a;
        padding: 0.5rem;
        border-radius: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 6px;
        color: #9ca3af;
        font-weight: 500;
        padding: 0.75rem 1.5rem;
        transition: all 0.2s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #0f0f0f;
        color: #e0e0e0;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #2563eb;
        color: #ffffff;
    }
    
    /* Download Button */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #059669 0%, #047857 100%);
        color: #ffffff;
        border: none;
        padding: 0.75rem 2rem;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.95rem;
        transition: all 0.3s ease;
        width: 100%;
        height: 48px;
    }
    
    .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #047857 0%, #065f46 100%);
        transform: translateY(-1px);
    }
    </style>
""", unsafe_allow_html=True)

# API Configuration
API_BASE_URL = "http://localhost:8000/api/v1"

# Initialize session state
def init_session_state():
    if 'zip_processed' not in st.session_state:
        st.session_state.zip_processed = False
    if 'processing_result' not in st.session_state:
        st.session_state.processing_result = None
    if 'selected_function' not in st.session_state:
        st.session_state.selected_function = None

init_session_state()

# ============================================
# UTILITY FUNCTIONS
# ============================================

def safe_decode(blob: bytes) -> str:
    """Safely decode bytes to string"""
    encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
    for e in encs:
        try:
            return blob.decode(e)
        except Exception:
            continue
    return blob.decode("utf-8", errors="replace")

def parse_registry_file(content: bytes) -> pd.DataFrame:
    """Parse registry file content into DataFrame"""
    lines = safe_decode(content).splitlines()
    
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

def detect_line_difference(line1: str, line2: str) -> str:
    """Detect type of difference between two lines"""
    if line1 == line2:
        return "identical"
    if line1.replace(' ', '').replace('\t', '') == line2.replace(' ', '').replace('\t', ''):
        return "whitespace"
    return "content"

def render_side_by_side_diff(content1: str, content2: str, filename1: str, filename2: str):
    """Render side-by-side diff with color coding"""
    lines1 = content1.splitlines()
    lines2 = content2.splitlines()
    
    max_lines = max(len(lines1), len(lines2))
    
    st.markdown("### File Comparison")
    st.caption(f"Comparing: {filename1} vs {filename2}")
    
    # Legend
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown('<div class="legend-item"><div class="legend-color" style="background-color: rgba(239, 68, 68, 0.15);"></div>Content Changes</div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="legend-item"><div class="legend-color" style="background-color: rgba(168, 85, 247, 0.12);"></div>Whitespace Only</div>', unsafe_allow_html=True)
    with col3:
        st.markdown('<div class="legend-item"><div class="legend-color" style="background-color: transparent; border: 1px solid #404040;"></div>Identical Lines</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown(f"#### {filename1}")
        html_left = '<div class="diff-pane"><div class="diff-pane-header">Original File</div>'
        
        for i in range(max_lines):
            line1 = lines1[i] if i < len(lines1) else ""
            line2 = lines2[i] if i < len(lines2) else ""
            
            diff_type = detect_line_difference(line1, line2)
            
            if diff_type == "content":
                css_class = "diff-content-change"
            elif diff_type == "whitespace":
                css_class = "diff-whitespace-change"
            else:
                css_class = "diff-identical"
            
            line1_escaped = line1.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            html_left += f'<div class="diff-line {css_class}"><span class="diff-line-number">{i+1}</span>{line1_escaped}</div>'
        
        html_left += '</div>'
        st.markdown(html_left, unsafe_allow_html=True)
    
    with col_right:
        st.markdown(f"#### {filename2}")
        html_right = '<div class="diff-pane"><div class="diff-pane-header">Modified File</div>'
        
        for i in range(max_lines):
            line1 = lines1[i] if i < len(lines1) else ""
            line2 = lines2[i] if i < len(lines2) else ""
            
            diff_type = detect_line_difference(line1, line2)
            
            if diff_type == "content":
                css_class = "diff-content-change"
            elif diff_type == "whitespace":
                css_class = "diff-whitespace-change"
            else:
                css_class = "diff-identical"
            
            line2_escaped = line2.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            html_right += f'<div class="diff-line {css_class}"><span class="diff-line-number">{i+1}</span>{line2_escaped}</div>'
        
        html_right += '</div>'
        st.markdown(html_right, unsafe_allow_html=True)

# ============================================
# ANALYSIS FUNCTIONS
# ============================================

def render_transaction_stats():
    """
    Render transaction statistics with source file filter
    """
    st.markdown("### 📊 Transaction Type Statistics")
    
    # Initialize a flag to track if we need to analyze
    need_analysis = False
    
    try:
        # STEP 1: Check if transaction data exists
        check_response = requests.get(
            f"{API_BASE_URL}/transaction-statistics",
            timeout=30
        )
        
        # If we get 400, it means data hasn't been analyzed yet
        if check_response.status_code == 400:
            need_analysis = True
            
            with st.spinner("Analyzing customer journals... This may take a moment."):
                try:
                    # Automatically analyze the customer journals
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        analyze_data = analyze_response.json()
                        # Give a moment for the session to update
                        import time
                        time.sleep(0.5)
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"❌ Failed to analyze customer journals: {error_detail}")
                        return
                        
                except requests.exceptions.Timeout:
                    st.error("⏱️ Analysis timeout. The file may be too large.")
                    return
                except requests.exceptions.ConnectionError:
                    st.error("🔌 Connection error. Ensure the API server is running on localhost:8000.")
                    return
                except Exception as e:
                    st.error(f"❌ Error during analysis: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                    return
        
        # STEP 2: Now get the statistics (either they existed or we just created them)
        response = requests.get(
            f"{API_BASE_URL}/transaction-statistics",
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # ========================================
            # SECTION 1: Overall Statistics
            # ========================================
            st.markdown("#### Overall Transaction Statistics")
            
            if 'statistics' in data:
                stats_df = pd.DataFrame(data['statistics'])
                
                # Display the statistics table
                st.dataframe(
                    stats_df,
                    use_container_width=True,
                    hide_index=True
                )
            
            # ========================================
            # SECTION 2: Source File Filter
            # ========================================
            st.markdown("---")
            
            try:
                # Get source files information
                sources_response = requests.get(
                    f"{API_BASE_URL}/get-transactions-with-sources",
                    timeout=30
                )
                
                if sources_response.status_code == 200:
                    sources_data = sources_response.json()
                    available_sources = sources_data.get('source_files', [])
                    
                    if available_sources:
                        
                        # Multi-select dropdown for source files
                        selected_sources = st.multiselect(
                            "Select source files to view their transactions",
                            options=available_sources,
                            default=None,
                            key="transaction_stats_sources",
                            help="Select one or more source files to filter transactions"
                        )
                        
                        if selected_sources:
                            
                            # Get filtered transactions
                            filter_response = requests.post(
                                f"{API_BASE_URL}/filter-transactions-by-sources",
                                json={"source_files": selected_sources},
                                timeout=30
                            )
                            
                            if filter_response.status_code == 200:
                                filtered_data = filter_response.json()
                                transactions = filtered_data.get('transactions', [])
                                
                                if transactions:
                                    
                                    # Create display DataFrame
                                    txn_display_data = []
                                    for txn in transactions:
                                        txn_display_data.append({
                                            'Transaction ID': txn.get('Transaction ID', 'N/A'),
                                            'Type': txn.get('Transaction Type', 'N/A'),
                                            'State': txn.get('End State', 'N/A'),
                                            'Duration (s)': txn.get('Duration (seconds)', 0),
                                            'Source File': txn.get('Source File', 'N/A'),
                                            'Start Time': txn.get('Start Time', 'N/A'),
                                            'End Time': txn.get('End Time', 'N/A')
                                        })
                                    
                                    txn_df = pd.DataFrame(txn_display_data)
                                    
                                    # Add additional filters
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        # Get unique transaction types
                                        unique_types = sorted(txn_df['Type'].unique().tolist())
                                        filter_type = st.selectbox(
                                            "Transaction Type",
                                            options=['All'] + unique_types,
                                            key="stats_type_filter"
                                        )
                                    
                                    with col2:
                                        # Get unique states
                                        unique_states = sorted(txn_df['State'].unique().tolist())
                                        filter_state = st.selectbox(
                                            "End State",
                                            options=['All'] + unique_states,
                                            key="stats_state_filter"
                                        )
                                    
                                    with col3:
                                        # Get unique source files from filtered data
                                        unique_sources = sorted(txn_df['Source File'].unique().tolist())
                                        if len(unique_sources) > 1:
                                            filter_source = st.selectbox(
                                                "Source File",
                                                options=['All'] + unique_sources,
                                                key="stats_source_filter"
                                            )
                                        else:
                                            filter_source = 'All'
                                    
                                    # Apply filters
                                    display_df = txn_df.copy()
                                    
                                    if filter_type != 'All':
                                        display_df = display_df[display_df['Type'] == filter_type]
                                    
                                    if filter_state != 'All':
                                        display_df = display_df[display_df['State'] == filter_state]
                                    
                                    if filter_source != 'All':
                                        display_df = display_df[display_df['Source File'] == filter_source]
                                    
                                    # Display filtered count
                                    if len(display_df) != len(txn_df):
                                        st.info(f"Filtered to {len(display_df)} transaction(s)")
                                    
                                    # Display the transactions table
                                    st.dataframe(
                                        display_df,
                                        use_container_width=True,
                                        hide_index=True
                                    )
                                    
                                    # Statistics for filtered data
                                    st.markdown("##### 📊 Statistics for Filtered Transactions")
                                    
                                    stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                                    
                                    with stat_col1:
                                        st.metric("Total", len(display_df))
                                    
                                    with stat_col2:
                                        successful = len(display_df[display_df['State'] == 'Successful'])
                                        st.metric("Successful", successful)
                                    
                                    with stat_col3:
                                        unsuccessful = len(display_df[display_df['State'] == 'Unsuccessful'])
                                        st.metric("Unsuccessful", unsuccessful)
                                    
                                    with stat_col4:
                                        if len(display_df) > 0:
                                            success_rate = (successful / len(display_df)) * 100
                                            st.metric("Success Rate", f"{success_rate:.1f}%")
                                        else:
                                            st.metric("Success Rate", "0%")
                                    
                                    # Download button
                                    st.markdown("---")
                                    csv = display_df.to_csv(index=False)
                                    st.download_button(
                                        label="📥 Download Filtered Transactions as CSV",
                                        data=csv,
                                        file_name=f"transactions_filtered_{len(selected_sources)}_sources.csv",
                                        mime="text/csv",
                                        key="download_filtered_txns"
                                    )
                                    
                                else:
                                    st.warning("⚠️ No transactions found for the selected source files.")
                            
                            else:
                                st.error(f"Failed to filter transactions. Status: {filter_response.status_code}")
                    
                    else:
                        st.warning("⚠️ No source files available. Please ensure customer journals were analyzed.")
                
                else:
                    st.error(f"Failed to retrieve source file information. Status: {sources_response.status_code}")
            
            except requests.exceptions.Timeout:
                st.error("⏱️ Request timeout while fetching source files. Please try again.")
            except requests.exceptions.ConnectionError:
                st.error("🔌 Connection error. Ensure the API server is running.")
            except Exception as e:
                st.error(f"❌ Error loading source file filter: {str(e)}")
        
        elif response.status_code == 400:
            # This shouldn't happen after our analysis, but just in case
            st.error("❌ Transaction data still not available after analysis. Please check the API logs.")
            error_detail = response.json().get('detail', 'Unknown error')
            st.info(f"Details: {error_detail}")
        
        else:
            st.error(f"Failed to load transaction statistics. Status code: {response.status_code}")
            try:
                error_detail = response.json().get('detail', 'Unknown error')
                st.info(f"Details: {error_detail}")
            except:
                pass
            
    except requests.exceptions.Timeout:
        st.error("⏱️ Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("🔌 Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"❌ Error loading transaction statistics: {str(e)}")
        import traceback
        with st.expander("🐛 Debug Information"):
            st.code(traceback.format_exc())

def render_individual_transaction_analysis():
    """Render individual transaction analysis"""
    st.markdown("### Individual Transaction Analysis")
    
    if 'transaction_analysis' not in st.session_state:
        if st.button("Load Transactions", use_container_width=True):
            with st.spinner("Loading transactions..."):
                try:
                    response = requests.get(f"{API_BASE_URL}/analyze-customer-journals")
                    
                    if response.status_code == 200:
                        analysis_data = response.json()
                        st.session_state['transaction_analysis'] = analysis_data
                        st.success("Transactions loaded successfully.")
                        st.rerun()
                    else:
                        st.error(f"Error: {response.json().get('detail', 'Unknown error occurred.')}")
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    else:
        analysis_data = st.session_state['transaction_analysis']
        transactions_df = pd.DataFrame(analysis_data['transactions'])
        
        transaction_options = [
            f"{row['Transaction ID']} - {row['Transaction Type']} ({row['End State']})"
            for _, row in transactions_df.iterrows()
        ]
        
        selected_txn_option = st.selectbox(
            "Select Transaction",
            options=["Select a transaction"] + transaction_options,
            key="selected_transaction"
        )
        
        if selected_txn_option != "Select a transaction":
            selected_txn_id = selected_txn_option.split(" - ")[0]
            txn_data = transactions_df[transactions_df['Transaction ID'] == selected_txn_id].iloc[0]
            
            st.markdown("---")
            st.markdown(f"### Transaction Details: {selected_txn_id}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Type", txn_data['Transaction Type'])
                st.metric("State", txn_data['End State'])
            with col2:
                st.metric("Start Time", txn_data['Start Time'])
                st.metric("End Time", txn_data['End Time'])
            with col3:
                st.metric("Duration", txn_data['Duration'])
                st.caption(f"Source: {txn_data['Source_File']}")
            
            st.markdown("---")
            st.markdown("### Transaction Log")
            st.code(txn_data['Transaction Log'], language='log')

def render_registry_single():
    """Render single registry file viewer"""
    st.markdown("### Registry File Viewer")

    file_categories = st.session_state.processing_result['categories']
    registry_files = file_categories.get('registry_files', {}).get('files', [])

    if not registry_files:
        st.warning("No registry files found in the uploaded package.")
        return

    file_map = {Path(f).name: f for f in registry_files}

    selected_file_name = st.selectbox(
        "Select Registry File",
        options=["Select a file"] + list(file_map.keys()),
        key="reg_single_select"
    )

    if selected_file_name != "Select a file":
        file_path = file_map[selected_file_name]
        
        with st.spinner("Loading registry file..."):
            try:
                with open(file_path, 'rb') as f:
                    content = f.read()
                
                df = parse_registry_file(content)
                
                if not df.empty:
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Entries", len(df))
                    with col2:
                        st.metric("Unique Paths", df['Path'].nunique())
                    with col3:
                        st.metric("Unique Keys", df['Key'].nunique())
                    
                    st.markdown("---")
                    search_term = st.text_input("Search Registry", placeholder="Search in path, key, or value", key="reg_search")
                    
                    display_df = df
                    if search_term:
                        mask = (
                            df['Path'].str.contains(search_term, case=False, na=False) |
                            df['Key'].str.contains(search_term, case=False, na=False) |
                            df['Value'].str.contains(search_term, case=False, na=False)
                        )
                        display_df = df[mask]
                        st.info(f"Found {len(display_df)} matching entries.")
                    
                    st.dataframe(display_df, use_container_width=True, height=400)
                    
                    csv = display_df.to_csv(index=False)
                    st.download_button(
                        label="Download as CSV",
                        data=csv,
                        file_name=f"{Path(selected_file_name).stem}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.warning("No entries found in the registry file.")
                    
            except Exception as e:
                st.error(f"Error loading file: {str(e)}")

def render_registry_compare():
    """Render Registry File Comparison - Upload 2 ZIPs"""
    st.markdown("### Registry File Comparison")
    
    # Check if we have any registry files in current session
    categories = st.session_state.processing_result['categories']
    registry_files_a = categories.get('registry_files', {}).get('files', [])
    
    if not registry_files_a:
        st.warning("No registry files found in the first uploaded package.")
        return
    
    st.markdown("#### Step 1: First Package (Already Loaded)")
    
    # Show available files from first package
    with st.expander("View files in first package"):
        for f in registry_files_a:
            st.caption(f"• {Path(f).name}")
    
    st.markdown("---")
    st.markdown("#### Step 2: Upload Second Package for Comparison")
    
    # File uploader for second ZIP
    uploaded_file_b = st.file_uploader(
        "Select second ZIP archive",
        type=['zip'],
        help="Upload another ZIP file to compare registry files",
        key="compare_zip_upload"
    )
    
    if uploaded_file_b is not None:
        file_size_mb = len(uploaded_file_b.getvalue()) / (1024 * 1024)
        
        # Process second ZIP button
        if st.button("Process Second Package", use_container_width=True, key="process_second_zip"):
            with st.spinner("Processing second package..."):
                try:
                    import requests
                    API_BASE_URL = "http://localhost:8000/api/v1"
                    
                    files = {"file": (uploaded_file_b.name, uploaded_file_b.getvalue(), "application/zip")}
                    response = requests.post(f"{API_BASE_URL}/process-zip", files=files, timeout=120)
                    
                    if response.status_code == 200:
                        result_b = response.json()
                        
                        # Store second package results in session state
                        st.session_state['compare_package_b'] = result_b
                        
                        registry_files_b = result_b['categories'].get('registry_files', {}).get('files', [])
                        
                        if not registry_files_b:
                            st.error("No registry files found in second package.")
                            return
                        
                        st.success(f"✓ Second package processed: {len(registry_files_b)} registry file(s) found")
                        st.rerun()
                    else:
                        st.error(f"Error processing second package: {response.json().get('detail')}")
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # If second package is loaded, show comparison UI
    if 'compare_package_b' in st.session_state:
        result_b = st.session_state['compare_package_b']
        registry_files_b = result_b['categories'].get('registry_files', {}).get('files', [])
        
        st.markdown("---")
        st.markdown("#### Step 3: Select Files to Compare")
        
        # Get file names from both packages
        files_a_map = {Path(f).name: f for f in registry_files_a}
        files_b_map = {Path(f).name: f for f in registry_files_b}
        
        # Find common file names
        common_names = set(files_a_map.keys()) & set(files_b_map.keys())
        
        if not common_names:
            st.warning("No files with matching names found in both packages.")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Files in Package 1:**")
                for name in sorted(files_a_map.keys()):
                    st.caption(f"• {name}")
            with col2:
                st.markdown("**Files in Package 2:**")
                for name in sorted(files_b_map.keys()):
                    st.caption(f"• {name}")
            
            # Reset button
            if st.button("Upload Different Package", use_container_width=True):
                del st.session_state['compare_package_b']
                st.rerun()
            return
        
        st.success(f"Found {len(common_names)} file(s) with matching names")
        
        # Select which file to compare
        selected_filename = st.selectbox(
            "Select file to compare",
            options=sorted(list(common_names)),
            key="compare_file_select"
        )
        
        if selected_filename:
            file_path_a = files_a_map[selected_filename]
            file_path_b = files_b_map[selected_filename]
            
            if st.button("Compare Selected Files", use_container_width=True, key="do_compare"):
                with st.spinner("Comparing files..."):
                    try:
                        # Read both files
                        with open(file_path_a, 'rb') as f:
                            content_a = f.read()
                        with open(file_path_b, 'rb') as f:
                            content_b = f.read()
                        
                        text_a = safe_decode(content_a)
                        text_b = safe_decode(content_b)
                        
                        fname_a = f"Package 1: {selected_filename}"
                        fname_b = f"Package 2: {selected_filename}"
                        render_side_by_side_diff(text_a, text_b, fname_a, fname_b)

                    except Exception as e:
                        st.error(f"Error comparing files: {str(e)}")
    

def render_transaction_comparison():
    """
    Render transaction comparison analysis with source file filter
    """
    st.markdown("### ⚖️ Transaction Comparison Analysis")
    
    need_analysis = False
    
    try:
        # ========================================
        # STEP 1: Check if analysis is needed
        # ========================================
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            need_analysis = True
        
        # STEP 2: Perform analysis if needed
        if need_analysis:
            st.info("📊 Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals... This may take a moment."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        analyze_data = analyze_response.json()
                        st.success(f"✓ Analysis complete! Found {analyze_data.get('total_transactions', 0)} transactions")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"❌ Failed to analyze customer journals: {error_detail}")
                        return
                        
                except requests.exceptions.Timeout:
                    st.error("⏱️ Analysis timeout. The file may be too large.")
                    return
                except requests.exceptions.ConnectionError:
                    st.error("🔌 Connection error. Ensure the API server is running on localhost:8000.")
                    return
                except Exception as e:
                    st.error(f"❌ Error during analysis: {str(e)}")
                    return
        
        # ========================================
        # SECTION 1: Source File Filter
        # ========================================
        # Get source files
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error("Failed to retrieve source file information.")
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        
        if not available_sources:
            st.warning("⚠️ No source files available. Please ensure customer journals were analyzed.")
            return
        
        # Multi-select dropdown for source files
        selected_sources = st.multiselect(
            "Choose source files containing transactions to compare",
            options=available_sources,
            default=available_sources,  # Select all by default
            key="comparison_sources",
            help="Select source files to filter available transactions"
        )
        
        if not selected_sources:
            st.info("👆 Please select at least one source file to continue")
            return
        
        # Get filtered transactions
        filter_response = requests.post(
            f"{API_BASE_URL}/filter-transactions-by-sources",
            json={"source_files": selected_sources},
            timeout=30
        )
        
        if filter_response.status_code != 200:
            st.error("Failed to get filtered transactions.")
            return
        
        filtered_data = filter_response.json()
        filtered_transactions = filtered_data.get('transactions', [])
        
        if len(filtered_transactions) < 2:
            st.warning(
                f"⚠️ Need at least 2 transactions for comparison. "
                f"Found only {len(filtered_transactions)} transaction(s) in the selected source files."
            )
            return
        
        # ========================================
        # SECTION 2: Optional Filters
        # ========================================
        st.markdown("---")
        
        # Create DataFrame for easier filtering
        txn_df = pd.DataFrame(filtered_transactions)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Filter by transaction type
            unique_types = sorted(txn_df['Transaction Type'].unique().tolist())
            if len(unique_types) > 1:
                filter_type = st.selectbox(
                    "Filter by Transaction Type",
                    options=['All Types'] + unique_types,
                    key="comparison_type_filter",
                    help="Optionally filter to compare transactions of the same type"
                )
                
                if filter_type != 'All Types':
                    txn_df = txn_df[txn_df['Transaction Type'] == filter_type]
                    filtered_transactions = txn_df.to_dict('records')
                    
                    if len(filtered_transactions) < 2:
                        st.warning(f"Only {len(filtered_transactions)} transaction(s) of type '{filter_type}'")
                        return
        
        with col2:
            # Filter by state
            unique_states = sorted(txn_df['End State'].unique().tolist())
            if len(unique_states) > 1:
                filter_state = st.selectbox(
                    "Filter by State",
                    options=['All States'] + unique_states,
                    key="comparison_state_filter",
                    help="Optionally filter by transaction state"
                )
                
                if filter_state != 'All States':
                    txn_df = txn_df[txn_df['End State'] == filter_state]
                    filtered_transactions = txn_df.to_dict('records')
                    
                    if len(filtered_transactions) < 2:
                        st.warning(f"Only {len(filtered_transactions)} transaction(s) with state '{filter_state}'")
                        return
        
        # ========================================
        # SECTION 3: Transaction Selection
        # ========================================
        st.markdown("---")
        st.markdown("#### 🔄 Select Two Transactions to Compare")
        
        col1, col2 = st.columns(2)
        
        # Transaction 1 selector
        with col1:
            st.markdown("##### First Transaction")
            txn1_options = [
                f"{txn['Transaction ID']} - {txn['Transaction Type']} ({txn['End State']})"
                for txn in filtered_transactions
            ]
            
            txn1_selection = st.selectbox(
                "Transaction 1",
                options=txn1_options,
                key="compare_txn1",
                help="Select the first transaction to compare"
            )
            
            if txn1_selection:
                txn1_id = txn1_selection.split(' - ')[0]
                txn1_data = next(
                    (txn for txn in filtered_transactions if txn['Transaction ID'] == txn1_id),
                    None
                )
                
                if txn1_data:
                    st.info(
                        f"**ID:** {txn1_data['Transaction ID']}\n\n"
                        f"**Type:** {txn1_data['Transaction Type']}\n\n"
                        f"**State:** {txn1_data['End State']}\n\n"
                        f"**Duration:** {txn1_data.get('Duration (seconds)', 0)}s\n\n"
                        f"**Source:** {txn1_data.get('Source File', 'Unknown')}"
                    )
        
        # Transaction 2 selector
        with col2:
            st.markdown("##### Second Transaction")
            
            # Filter out the first selected transaction
            txn2_options = [
                opt for opt in txn1_options
                if opt.split(' - ')[0] != (txn1_id if txn1_selection else None)
            ]
            
            if not txn2_options:
                st.warning("No other transactions available for comparison")
                return
            
            txn2_selection = st.selectbox(
                "Transaction 2",
                options=txn2_options,
                key="compare_txn2",
                help="Select the second transaction to compare"
            )
            
            if txn2_selection:
                txn2_id = txn2_selection.split(' - ')[0]
                txn2_data = next(
                    (txn for txn in filtered_transactions if txn['Transaction ID'] == txn2_id),
                    None
                )
                
                if txn2_data:
                    st.info(
                        f"**ID:** {txn2_data['Transaction ID']}\n\n"
                        f"**Type:** {txn2_data['Transaction Type']}\n\n"
                        f"**State:** {txn2_data['End State']}\n\n"
                        f"**Duration:** {txn2_data.get('Duration (seconds)', 0)}s\n\n"
                        f"**Source:** {txn2_data.get('Source File', 'Unknown')}"
                    )
        
        # Check if both transactions are selected
        if not (txn1_selection and txn2_selection):
            st.info("👆 Please select both transactions above to proceed with comparison")
            return
        
        # ========================================
        # SECTION 4: Perform Comparison
        # ========================================
        st.markdown("---")
        st.markdown("#### 📊 Comparison Results")
        
        with st.spinner(f"Comparing {txn1_id} and {txn2_id}..."):
            try:
                # Call comparison API
                comparison_response = requests.post(
                    f"{API_BASE_URL}/compare-transactions-flow",
                    json={
                        "txn1_id": txn1_id,
                        "txn2_id": txn2_id
                    },
                    timeout=30
                )
                
                if comparison_response.status_code == 200:
                    comparison_data = comparison_response.json()
                    
                    # Create tabs for different views
                    tab1, tab2, tab3 = st.tabs([
                        "📊 Side-by-Side Flow",
                        "📝 Transaction Logs",
                        "📈 Detailed Analysis"
                    ])
                    
                    # ========================================
                    # TAB 1: Side-by-Side Flow Comparison
                    # ========================================
                    with tab1:
                        st.markdown("#### Side-by-Side UI Flow Comparison")
                        
                        txn1_flow = comparison_data.get('txn1_flow', [])
                        txn2_flow = comparison_data.get('txn2_flow', [])
                        txn1_matches = comparison_data.get('txn1_matches', [])
                        txn2_matches = comparison_data.get('txn2_matches', [])
                        
                        # Display flows side by side
                        flow_col1, flow_col2 = st.columns(2)
                        
                        with flow_col1:
                            st.markdown(f"##### Transaction 1: {txn1_id}")
                            st.caption(f"State: {comparison_data.get('txn1_state', 'Unknown')}")
                            st.caption(f"{len(txn1_flow)} screen(s)")
                            
                            for i, (screen, is_match) in enumerate(zip(txn1_flow, txn1_matches), 1):
                                if is_match:
                                    st.success(f"**{i}.** {screen}")
                                else:
                                    st.warning(f"**{i}.** {screen}")
                        
                        with flow_col2:
                            st.markdown(f"##### Transaction 2: {txn2_id}")
                            st.caption(f"State: {comparison_data.get('txn2_state', 'Unknown')}")
                            st.caption(f"{len(txn2_flow)} screen(s)")
                            
                            for i, (screen, is_match) in enumerate(zip(txn2_flow, txn2_matches), 1):
                                if is_match:
                                    st.success(f"**{i}.** {screen}")
                                else:
                                    st.warning(f"**{i}.** {screen}")
                        
                        # Legend
                        st.markdown("---")
                        legend_col1, legend_col2 = st.columns(2)
                        with legend_col1:
                            st.success("Screen appears in both transactions")
                        with legend_col2:
                            st.warning("Screen unique to this transaction")
                        
                        # Calculate and display similarity metrics
                        st.markdown("---")
                        st.markdown("##### 🎯 Flow Similarity Metrics")
                        
                        common_screens = len(set(txn1_flow) & set(txn2_flow))
                        total_unique_screens = len(set(txn1_flow) | set(txn2_flow))
                        similarity = (common_screens / total_unique_screens * 100) if total_unique_screens > 0 else 0
                        
                        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                        
                        with metric_col1:
                            st.metric("Common Screens", common_screens)
                        with metric_col2:
                            st.metric("Different Screens", total_unique_screens - common_screens)
                        with metric_col3:
                            st.metric("Total Unique Screens", total_unique_screens)
                        with metric_col4:
                            st.metric("Similarity", f"{similarity:.1f}%")
                    
                    # ========================================
                    # TAB 2: Transaction Logs
                    # ========================================
                    with tab2:
                        st.markdown("#### Transaction Logs Comparison")
                        
                        log_col1, log_col2 = st.columns(2)
                        
                        with log_col1:
                            st.markdown(f"##### Transaction 1: {txn1_id}")
                            txn1_log = comparison_data.get('txn1_log', 'No log available')
                            st.code(txn1_log, language="log", line_numbers=True)
                        
                        with log_col2:
                            st.markdown(f"##### Transaction 2: {txn2_id}")
                            txn2_log = comparison_data.get('txn2_log', 'No log available')
                            st.code(txn2_log, language="log", line_numbers=True)
                    
                    # ========================================
                    # TAB 3: Detailed Analysis
                    # ========================================
                    with tab3:
                        st.markdown("#### Detailed Comparison Analysis")
                        
                        # Additional comparison metrics
                        st.markdown("##### 📊 Detailed Metrics")
                        
                        # Duration comparison
                        if txn1_data and txn2_data:
                            duration_col1, duration_col2, duration_col3 = st.columns(3)
                            
                            with duration_col1:
                                txn1_duration = txn1_data.get('Duration (seconds)', 0)
                                st.metric("Transaction 1 Duration", f"{txn1_duration}s")
                            
                            with duration_col2:
                                txn2_duration = txn2_data.get('Duration (seconds)', 0)
                                st.metric("Transaction 2 Duration", f"{txn2_duration}s")
                            
                            with duration_col3:
                                duration_diff = txn2_duration - txn1_duration
                                st.metric(
                                    "Duration Difference",
                                    f"{abs(duration_diff)}s",
                                    delta=f"TXN2 {'slower' if duration_diff > 0 else 'faster'}"
                                )
                        
                        st.markdown("---")
                        
                        # Source file comparison
                        st.markdown("##### 📁 Source File Information")
                        source_col1, source_col2 = st.columns(2)
                        
                        with source_col1:
                            st.info(f"**Transaction 1 Source:**\n\n{txn1_data.get('Source File', 'Unknown')}")
                        
                        with source_col2:
                            st.info(f"**Transaction 2 Source:**\n\n{txn2_data.get('Source File', 'Unknown')}")
                        
                        # Check if from same source
                        if txn1_data.get('Source File') == txn2_data.get('Source File'):
                            st.success("Both transactions are from the same source file")
                        else:
                            st.warning("Transactions are from different source files")
                        
                        st.markdown("---")
                        
                        # Screen-by-screen comparison
                        st.markdown("##### 🔍 Screen-by-Screen Breakdown")
                        
                        # Unique to Transaction 1
                        unique_to_txn1 = set(txn1_flow) - set(txn2_flow)
                        if unique_to_txn1:
                            with st.expander(f"Screens unique to {txn1_id} ({len(unique_to_txn1)})"):
                                for screen in sorted(unique_to_txn1):
                                    st.markdown(f"- {screen}")
                        else:
                            st.info(f"No screens unique to {txn1_id}")
                        
                        # Unique to Transaction 2
                        unique_to_txn2 = set(txn2_flow) - set(txn1_flow)
                        if unique_to_txn2:
                            with st.expander(f"Screens unique to {txn2_id} ({len(unique_to_txn2)})"):
                                for screen in sorted(unique_to_txn2):
                                    st.markdown(f"- {screen}")
                        else:
                            st.info(f"No screens unique to {txn2_id}")
                        
                        # Common screens
                        common = set(txn1_flow) & set(txn2_flow)
                        if common:
                            with st.expander(f"Common screens ({len(common)})", expanded=True):
                                for screen in sorted(common):
                                    st.markdown(f"- {screen}")
                
                elif comparison_response.status_code == 404:
                    error_detail = comparison_response.json().get('detail', 'Transaction not found')
                    st.error(f"❌ {error_detail}")
                elif comparison_response.status_code == 400:
                    error_detail = comparison_response.json().get('detail', 'Bad request')
                    st.error(f"❌ {error_detail}")
                else:
                    st.error(f"Failed to compare transactions. Status code: {comparison_response.status_code}")
                    
            except requests.exceptions.Timeout:
                st.error("⏱️ Request timeout while comparing transactions. Please try again.")
            except requests.exceptions.ConnectionError:
                st.error("🔌 Connection error. Ensure the API server is running.")
            except Exception as e:
                st.error(f"❌ Error during comparison: {str(e)}")
                import traceback
                with st.expander("🐛 Debug Information"):
                    st.code(traceback.format_exc())
    
    except requests.exceptions.Timeout:
        st.error("⏱️ Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("🔌 Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"❌ Error in transaction comparison: {str(e)}")
        import traceback
        with st.expander("🐛 Debug Information"):
            st.code(traceback.format_exc())

def render_ui_flow_individual():
    """Render UI flow visualization for individual transaction"""
    st.markdown("### 🖥️ UI Flow of Individual Transaction")
    
    need_analysis = False
    
    try:
        # STEP 1: Try to get source files - if it fails, we need to analyze
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                # Check if we actually have sources
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            st.error(f"Error checking for source files: {str(e)}")
            need_analysis = True
        
        # STEP 2: If we need analysis, do it now
        if need_analysis:
            st.info("📊 Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals... This may take a moment."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        analyze_data = analyze_response.json()
                        st.success(f"✓ Analysis complete! Found {analyze_data.get('total_transactions', 0)} transactions")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"❌ Failed to analyze customer journals: {error_detail}")
                        return
                        
                except requests.exceptions.Timeout:
                    st.error("⏱️ Analysis timeout. The file may be too large.")
                    return
                except requests.exceptions.ConnectionError:
                    st.error("🔌 Connection error. Ensure the API server is running on localhost:8000.")
                    return
                except Exception as e:
                    st.error(f"❌ Error during analysis: {str(e)}")
                    import traceback
                    with st.expander("🐛 Debug Information"):
                        st.code(traceback.format_exc())
                    return
        
        # STEP 3: Get source files again after potential analysis
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error(f"Failed to retrieve source files. Status: {sources_response.status_code}")
            try:
                error_detail = sources_response.json().get('detail', 'Unknown error')
                st.info(f"Details: {error_detail}")
            except:
                pass
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_transactions = sources_data.get('all_transactions', [])
        
        if not available_sources:
            st.warning("⚠️ No source files found even after analysis.")
            st.info("Please check:\n1. ZIP file contains customer journal files\n2. Customer journal files contain valid transaction data")
            return
        
        # STEP 4: Display source file selection
        
        selected_sources = st.multiselect(
            "Select one or more source files to view their transactions",
            options=available_sources,
            default=None,
            key="ui_flow_sources",
            help="Select source files to filter transactions"
        )
        
        if not selected_sources:
            return
        
        # STEP 5: Filter transactions by selected sources
        
        filter_response = requests.post(
            f"{API_BASE_URL}/filter-transactions-by-sources",
            json={"source_files": selected_sources},
            timeout=30
        )
        
        if filter_response.status_code != 200:
            st.error(f"Failed to filter transactions. Status: {filter_response.status_code}")
            try:
                error_detail = filter_response.json().get('detail', 'Unknown error')
                st.info(f"Details: {error_detail}")
            except:
                pass
            return
        
        filtered_data = filter_response.json()
        filtered_transactions = filtered_data.get('transactions', [])
        
        if not filtered_transactions:
            st.warning("⚠️ No transactions found for the selected source files.")
            return
        
        # Convert to DataFrame for easier filtering
        txn_df = pd.DataFrame(filtered_transactions)
        
        # STEP 6: Add filters for Type and State
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Get unique transaction types
            unique_types = sorted(txn_df['Transaction Type'].unique().tolist())
            filter_type = st.selectbox(
                "Transaction Type (Optional)",
                options=['All'] + unique_types,
                key="ui_flow_type_filter"
            )
        
        with col2:
            # Get unique states
            unique_states = sorted(txn_df['End State'].unique().tolist())
            filter_state = st.selectbox(
                "End State (Optional)",
                options=['All'] + unique_states,
                key="ui_flow_state_filter"
            )
        
        # Apply filters
        display_df = txn_df.copy()
        
        if filter_type != 'All':
            display_df = display_df[display_df['Transaction Type'] == filter_type]
        
        if filter_state != 'All':
            display_df = display_df[display_df['End State'] == filter_state]
        
        if len(display_df) == 0:
            st.warning("⚠️ No transactions match the selected filters.")
            return
        
        # Display filtered count
        if len(display_df) != len(txn_df):
            st.info(f"Filtered to {len(display_df)} transaction(s)")
        
        # Create options for selectbox
        transaction_options = []
        for _, txn in display_df.iterrows():
            txn_id = txn.get('Transaction ID', 'N/A')
            txn_type = txn.get('Transaction Type', 'Unknown')
            txn_state = txn.get('End State', 'Unknown')
            source_file = txn.get('Source File', 'Unknown')
            start_time = txn.get('Start Time', 'N/A')
            transaction_options.append(f"{txn_id} | {txn_type} | {txn_state} | {start_time} | {source_file}")
        
        selected_option = st.selectbox(
            "Select a transaction to visualize",
            options=["Select a transaction..."] + transaction_options,
            key="ui_flow_transaction_select"
        )
        
        if selected_option == "Select a transaction...":
            return
        
        # Extract transaction ID from selected option
        selected_txn_id = selected_option.split(" | ")[0]
        
        # STEP 8: Get and display the UI flow
        st.markdown("---")
        
        with st.spinner(f"Loading UI flow for transaction {selected_txn_id}..."):
            try:
                viz_response = requests.post(
                    f"{API_BASE_URL}/visualize-individual-transaction-flow",
                    json={"transaction_id": selected_txn_id},
                    timeout=60
                )
                
                if viz_response.status_code == 200:
                    viz_data = viz_response.json()
                    
                    # Display transaction details
                    st.markdown(f"### Transaction: {viz_data['transaction_id']}")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Type", viz_data['transaction_type'])
                    with col2:
                        st.metric("State", viz_data['end_state'])
                    with col3:
                        st.metric("UI Events", viz_data.get('num_events', 0))
                    
                    col4, col5, col6 = st.columns(3)
                    with col4:
                        st.metric("Start Time", viz_data.get('start_time', 'N/A'))
                    with col5:
                        st.metric("End Time", viz_data.get('end_time', 'N/A'))
                    with col6:
                        st.metric("Source File", viz_data.get('source_file', 'N/A'))
                    
                    # Display UI flow
                    st.markdown("---")
                    st.markdown("#### 🖥️ UI Flow Visualization")
                    
                    ui_flow = viz_data.get('ui_flow', [])
                    has_flow = viz_data.get('has_flow', False)
                    
                    if has_flow and ui_flow:
                        
                        # Create and display the flowchart
                        fig = create_individual_flow_plotly(
                            viz_data['transaction_id'],
                            viz_data['end_state'],
                            ui_flow
                        )
                        
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            # Fallback to list view
                            for idx, screen in enumerate(ui_flow, 1):
                                st.markdown(f"**{idx}.** {screen}")
                    else:
                        st.warning("⚠️ No UI flow data available for this transaction")
                        st.info("This could mean:\n- No UI journal files were uploaded\n- The transaction time range doesn't match any UI events\n- UI journal data is incomplete")
                    
                    # Show transaction log
                    st.markdown("---")
                    st.markdown("#### 📋 Transaction Log")
                    with st.expander("View Full Transaction Log", expanded=False):
                        st.code(viz_data.get('transaction_log', 'No log available'), language="text")
                
                else:
                    error_detail = viz_response.json().get('detail', 'Visualization failed')
                    st.error(f"❌ {error_detail}")
                    
            except requests.exceptions.Timeout:
                st.error("⏱️ Request timeout. Please try again.")
            except requests.exceptions.ConnectionError:
                st.error("🔌 Connection error. Ensure the API server is running on localhost:8000.")
            except Exception as e:
                st.error(f"❌ Error in UI flow visualization: {str(e)}")
                import traceback
                with st.expander("🐛 Debug Information"):
                    st.code(traceback.format_exc())
    
    except requests.exceptions.Timeout:
        st.error("⏱️ Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("🔌 Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"❌ Error loading UI flow: {str(e)}")
        import traceback
        with st.expander("🐛 Debug Information"):
            st.code(traceback.format_exc())

def render_under_construction(function_name: str):
    """Render under construction message"""
    st.markdown(f"### {function_name}")
    st.warning("This feature is currently under development.")

def create_individual_flow_plotly(txn_id, txn_state, flow_screens):
    """Create a Plotly visualization for individual transaction flow"""
    import plotly.graph_objects as go
    
    if not flow_screens or flow_screens[0] == 'No flow data':
        return None
    
    # Color based on transaction state
    if txn_state == 'Successful':
        box_color = '#2563eb'
    elif txn_state == 'Unsuccessful':
        box_color = '#2563eb'
    else:
        box_color = '#2563eb'
    
    fig = go.Figure()
    
    max_screens = len(flow_screens)
    
    # Add boxes for each screen
    for i, screen in enumerate(flow_screens):
        y_pos = max_screens - 1 - i
        
        # Add box
        fig.add_shape(
            type="rect",
            x0=0.1, x1=0.9, y0=y_pos, y1=y_pos + 0.7,
            fillcolor=box_color,
            line=dict(color=box_color, width=2)
        )
        
        # Add text
        fig.add_annotation(
            x=0.5, y=y_pos + 0.35,
            text=f"{i+1}. {screen}",
            showarrow=False,
            font=dict(color="white", size=11, family="Arial"),
            xanchor="center", yanchor="middle"
        )
        
        # Add arrow to next screen (if not last)
        if i < len(flow_screens) - 1:
            fig.add_annotation(
                x=0.5, y=(y_pos - 1) + 0.7, # Arrow tip: top of the next box
                ax=0.5, ay=y_pos,           # Arrow tail: bottom of the current box
                xref='x', yref='y',
                axref='x', ayref='y',
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor=box_color
            )
    
    # Calculate height based on number of screens
    height = max(400, max_screens * 100)
    
    # Update layout
    fig.update_layout(
        height=height,
        showlegend=False,
        plot_bgcolor='#0E1117',
        paper_bgcolor='#0E1117',
        font=dict(color='white'),
        title=dict(
            text=f"UI Flow: {txn_id}",
            font=dict(size=16, color='white'),
            x=0.5,
            xanchor='center'
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    # Update axes
    fig.update_xaxes(
        showgrid=False, 
        showticklabels=False, 
        zeroline=False,
        range=[0, 1]
    )
    fig.update_yaxes(
        showgrid=False, 
        showticklabels=False, 
        zeroline=False,
        range=[-0.5, max_screens]
    )
    
    return fig

def create_consolidated_flow_plotly(flow_data):
    """Create consolidated flow visualization showing all transactions of a type"""
    import plotly.graph_objects as go
    from collections import defaultdict
    
    screens = flow_data['screens']
    transitions = flow_data['transitions']
    screen_transactions = flow_data['screen_transactions']
    
    if not screens:
        return None
    
    # Define screen hierarchy for positioning
    screen_hierarchy = {
        'Login': 0, 'PIN': 0, 'Authentication': 0, 'CardInsert': 0,
        'MainMenu': 1, 'Menu': 1, 'MenuSelection': 1,
        'Balance': 2, 'BalanceInquiry': 2, 'Inquiry': 2,
        'Withdraw': 2, 'Transfer': 2, 'Deposit': 2,
        'Amount': 3, 'Account': 3, 'FromAccount': 3, 'ToAccount': 3,
        'Confirm': 4, 'Confirmation': 4, 'Verify': 4,
        'Authorize': 5, 'Authorization': 5, 'DMAuthorization': 5,
        'Processing': 6, 'Wait': 6, 'PleaseWait': 6,
        'Cash': 7, 'Card': 7, 'Dispense': 7,
        'Receipt': 8, 'ReceiptPrint': 8, 'Print': 8,
        'End': 9, 'Complete': 9, 'Success': 9, 'ThankYou': 9,
        'Error': 10, 'Cancel': 10, 'Failed': 10, 'Timeout': 10
    }
    
    # Sort screens by hierarchy
    screens_list = sorted(screens, key=lambda x: (screen_hierarchy.get(x, 99), x))
    
    # Calculate grid layout
    cols = 3
    rows = (len(screens_list) + cols - 1) // cols
    
    cell_width, cell_height = 20, 12
    positions = {}
    
    for i, screen in enumerate(screens_list):
        col = i % cols
        row = i // cols
        x = col * cell_width
        y = -row * cell_height
        positions[screen] = (x, y)
    
    fig = go.Figure()
    
    # Add screen boxes
    for screen in screens_list:
        x, y = positions[screen]
        txn_list = screen_transactions.get(screen, [])
        
        # Create hover text
        hover_text = f"<b>{screen}</b><br><br><b>{len(txn_list)} transactions</b><br>"
        for i, txn_info in enumerate(txn_list[:5]):
            hover_text += f"• {txn_info['txn_id']} - {txn_info['start_time']} ({txn_info['state']})<br>"
        if len(txn_list) > 5:
            hover_text += f"...and {len(txn_list) - 5} more"
        
        # Determine color based on screen type
        if any(term in screen.lower() for term in ['error', 'fail', 'cancel', 'timeout']):
            color = '#ffb3b3'  # Light red
        elif any(term in screen.lower() for term in ['receipt', 'complete', 'success', 'end', 'thankyou']):
            color = '#b3ffb3'  # Light green
        else:
            color = '#b3d9ff'  # Light blue
        
        # Add rectangle
        fig.add_shape(
            type="rect",
            x0=x - 8, x1=x + 8,
            y0=y - 6, y1=y + 6,
            line=dict(color='black', width=2),
            fillcolor=color,
            layer="below"
        )
        
        # Add text
        fig.add_trace(go.Scatter(
            x=[x], y=[y],
            text=[f"<b>{screen}</b>"],
            mode='text',
            textfont=dict(size=10, family='Arial', color='black'),
            hovertemplate=hover_text + '<extra></extra>',
            showlegend=False
        ))
    
    # Add transitions (arrows with counts)
    for transition in transitions:
        from_screen = transition['from']
        to_screen = transition['to']
        count = transition['count']
        
        if from_screen in positions and to_screen in positions:
            x0, y0 = positions[from_screen]
            x1, y1 = positions[to_screen]
            
            # Add arrow
            fig.add_annotation(
                x=x1, y=y1, ax=x0, ay=y0,
                xref='x', yref='y',
                axref='x', ayref='y',
                showarrow=True,
                arrowhead=2,
                arrowsize=1.2,
                arrowwidth=2.5,
                arrowcolor='green'
            )
            
            # Add count label
            fig.add_annotation(
                x=(x0 + x1)/2,
                y=(y0 + y1)/2,
                text=f"<b>{count}</b>",
                showarrow=False,
                font=dict(size=12, color="black", family="Arial Black"),
                align="center",
                bordercolor="black",
                borderwidth=1,
                bgcolor="white",
                opacity=1
            )
    
    # Update layout
    all_x = [p[0] for p in positions.values()]
    all_y = [p[1] for p in positions.values()]
    
    fig.update_layout(
        title=dict(
            text=f"<b>Consolidated Flow:</b> {flow_data['transaction_type']}<br><sub>({flow_data['transactions_with_flow']} transactions)</sub>",
            font=dict(size=22, color='black'),
            x=0.5,
            xanchor='center'
        ),
        xaxis=dict(
            showgrid=False, 
            zeroline=False, 
            showticklabels=False, 
            range=[min(all_x)-15, max(all_x)+15]
        ),
        yaxis=dict(
            showgrid=False, 
            zeroline=False, 
            showticklabels=False, 
            range=[min(all_y)-15, max(all_y)+15]
        ),
        height=rows * 200 + 100,
        width=cols * 300 + 100,
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(t=120, l=50, r=50, b=50)
    )
    
    return fig

def render_consolidated_flow():
    """Render consolidated transaction flow analysis"""
    st.markdown("### 🌐 Consolidated Transaction UI Flow and Analysis")
    
    need_analysis = False
    
    try:
        # STEP 1: Check if analysis is needed
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            need_analysis = True
        
        # STEP 2: Perform analysis if needed
        if need_analysis:
            st.info("📊 Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals..."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        st.success("✓ Analysis complete!")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"❌ {error_detail}")
                        return
                except Exception as e:
                    st.error(f"❌ Error during analysis: {str(e)}")
                    return
        
        # STEP 3: Get source files
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error("Failed to retrieve source files")
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_transactions = sources_data.get('all_transactions', [])
        
        if not available_sources:
            st.warning("⚠️ No source files available")
            return
        
        selected_source = st.selectbox(
            "Source File",
            options=available_sources,
            key="consolidated_source"
        )
        
        if not selected_source:
            return
        
        # Filter transactions from this source
        source_transactions = [txn for txn in all_transactions if txn.get('Source File') == selected_source]
        
        if not source_transactions:
            st.warning(f"No transactions found in source '{selected_source}'")
            return
        
        # Get unique transaction types from this source
        txn_df = pd.DataFrame(source_transactions)
        unique_types = sorted(txn_df['Transaction Type'].dropna().unique().tolist())
        
        if not unique_types:
            st.warning("No transaction types found in this source")
            return
        
        # STEP 5: Select transaction type
        st.markdown("---")
        
        selected_type = st.selectbox(
            "Transaction Type",
            options=unique_types,
            key="consolidated_type"
        )
        
        # Show stats for selected type
        type_transactions = txn_df[txn_df['Transaction Type'] == selected_type]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Transactions", len(type_transactions))
        with col2:
            successful = len(type_transactions[type_transactions['End State'] == 'Successful'])
            st.metric("Successful", successful)
        with col3:
            unsuccessful = len(type_transactions[type_transactions['End State'] == 'Unsuccessful'])
            st.metric("Unsuccessful", unsuccessful)
        
        # STEP 6: Generate consolidated flow
        st.markdown("---")
        
        if st.button("🌐 Generate Consolidated Flow", use_container_width=True):
            with st.spinner(f"Generating consolidated flow for {selected_type}..."):
                try:
                    response = requests.post(
                        f"{API_BASE_URL}/generate-consolidated-flow",
                        json={
                            "source_file": selected_source,
                            "transaction_type": selected_type
                        },
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        flow_data = response.json()
                        
                        # Display the consolidated flow chart
                        st.markdown("---")
                        st.markdown("### 🌐 Consolidated Flow Visualization")
                        st.info("💡 Hover over screens to see transaction IDs. Arrows show flow direction with transaction counts.")
                        
                        fig = create_consolidated_flow_plotly(flow_data)
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # Show detailed flow information
                        with st.expander("📊 Transaction Flow Details"):
                            st.markdown(f"**Transactions with UI flow data:** {flow_data['transactions_with_flow']}/{flow_data['total_transactions']}")
                            
                            st.markdown("**Individual Transaction Flows:**")
                            for txn_id, flow_info in flow_data['transaction_flows'].items():
                                st.markdown(
                                    f"• **{txn_id}** ({flow_info['state']}) "
                                    f"[{flow_info['start_time']} - {flow_info['end_time']}]: "
                                    f"{' → '.join(flow_info['screens'])}"
                                )
                    
                    else:
                        error_detail = response.json().get('detail', 'Failed to generate flow')
                        st.error(f"❌ {error_detail}")
                        
                except requests.exceptions.Timeout:
                    st.error("⏱️ Request timeout. Please try again.")
                except requests.exceptions.ConnectionError:
                    st.error("🔌 Connection error. Ensure the API server is running.")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    import traceback
                    with st.expander("🐛 Debug Information"):
                        st.code(traceback.format_exc())
    
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        import traceback
        with st.expander("🐛 Debug Information"):
            st.code(traceback.format_exc())

def render_individual_transaction_analysis():
    """Render individual transaction analysis with LLM"""
    st.markdown("### 🔍 Individual Transaction Analysis")
    
    need_analysis = False
    
    try:
        # STEP 1: Check if analysis is needed
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            need_analysis = True
        
        # STEP 2: Perform analysis if needed
        if need_analysis:
            st.info("📊 Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals..."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        st.success("✓ Analysis complete!")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"❌ {error_detail}")
                        return
                except Exception as e:
                    st.error(f"❌ Error during analysis: {str(e)}")
                    return
        
        # STEP 3: Get source files and transactions
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error("Failed to retrieve transaction data")
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_transactions = sources_data.get('all_transactions', [])
        
        if not available_sources:
            st.warning("⚠️ No source files available")
            return
        
        if not all_transactions:
            st.warning("⚠️ No transactions available")
            return
        
        # STEP 4: Filters
        st.markdown("#### 🔍 Select Transaction")
        
        txn_df = pd.DataFrame(all_transactions)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Source file filter
            selected_sources = st.multiselect(
                "Source Files",
                options=available_sources,
                default=available_sources,
                key="indiv_analysis_sources"
            )
        
        with col2:
            # Transaction type filter
            if selected_sources:
                filtered_df = txn_df[txn_df['Source File'].isin(selected_sources)]
                unique_types = sorted(filtered_df['Transaction Type'].dropna().unique().tolist())
                
                selected_type = st.selectbox(
                    "Transaction Type",
                    options=['All Types'] + unique_types,
                    key="indiv_analysis_type"
                )
                
                if selected_type != 'All Types':
                    filtered_df = filtered_df[filtered_df['Transaction Type'] == selected_type]
            else:
                filtered_df = pd.DataFrame()
        
        with col3:
            # State filter
            if not filtered_df.empty:
                unique_states = sorted(filtered_df['End State'].dropna().unique().tolist())
                
                selected_state = st.selectbox(
                    "End State",
                    options=['All States'] + unique_states,
                    key="indiv_analysis_state"
                )
                
                if selected_state != 'All States':
                    filtered_df = filtered_df[filtered_df['End State'] == selected_state]
        
        if filtered_df.empty:
            st.warning("⚠️ No transactions match the selected filters")
            return
        
        # STEP 5: Transaction selection
        st.markdown("---")
        st.markdown("#### 📋 Select a Transaction to Analyze")
        
        # Create transaction options
        transaction_options = {}
        for _, txn in filtered_df.iterrows():
            txn_id = txn['Transaction ID']
            display = f"{txn_id} | {txn['Transaction Type']} | {txn['End State']} | {txn['Start Time']}"
            transaction_options[display] = txn_id
        
        selected_display = st.selectbox(
            "Transaction",
            options=list(transaction_options.keys()),
            key="indiv_analysis_txn_select"
        )
        
        selected_txn_id = transaction_options[selected_display]
        selected_txn_data = filtered_df[filtered_df['Transaction ID'] == selected_txn_id].iloc[0]
        
        # STEP 6: Display transaction details
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📋 Transaction Details")
            st.markdown(f"**ID:** {selected_txn_data['Transaction ID']}")
            st.markdown(f"**Type:** {selected_txn_data['Transaction Type']}")
            st.markdown(f"**State:** {selected_txn_data['End State']}")
            st.markdown(f"**Start Time:** {selected_txn_data['Start Time']}")
            st.markdown(f"**End Time:** {selected_txn_data['End Time']}")
            st.markdown(f"**Source File:** {selected_txn_data['Source File']}")
        
        with col2:
            st.markdown("#### 📝 Transaction Log Preview")
            transaction_log = str(selected_txn_data.get('Transaction Log', 'No log available'))
            
            # Show first 500 characters as preview
            preview = transaction_log[:500] + "..." if len(transaction_log) > 500 else transaction_log
            st.text_area(
                "Log Preview",
                preview,
                height=200,
                disabled=True,
                key="log_preview"
            )
        
        # STEP 7: LLM Analysis
        st.markdown("---")
        st.markdown("### DN Transaction Analysis")
        
        # Initialize session state for analysis
        if 'current_analysis_txn' not in st.session_state:
            st.session_state.current_analysis_txn = None
        if 'analysis_result' not in st.session_state:
            st.session_state.analysis_result = None
        
        # Check if we need to clear previous analysis
        if st.session_state.current_analysis_txn != selected_txn_id:
            st.session_state.analysis_result = None
            st.session_state.current_analysis_txn = selected_txn_id
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            analyze_button = st.button(
                "🤖 Analyze Transaction",
                use_container_width=True,
                type="primary"
            )
        
        with col2:
            if st.session_state.analysis_result:
                print("")
        
        if analyze_button:
            with st.spinner("🤖 DN Analyzer is analyzing the transaction log... This may take a moment."):
                try:
                    response = requests.post(
                        f"{API_BASE_URL}/analyze-transaction-llm",
                        json={"transaction_id": selected_txn_id},
                        timeout=120  # LLM can take time
                    )
                    
                    if response.status_code == 200:
                        st.session_state.analysis_result = response.json()
                        st.rerun()
                    else:
                        error_detail = response.json().get('detail', 'Analysis failed')
                        st.error(f"❌ {error_detail}")
                        
                except requests.exceptions.Timeout:
                    st.error("⏱️ Analysis timeout. The model may be taking too long to respond.")
                except requests.exceptions.ConnectionError:
                    st.error("🔌 Connection error. Ensure the API server and Ollama are running.")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        
        # Display analysis results
        if st.session_state.analysis_result:
            st.markdown("---")
            st.markdown("### 📊 Analysis Results")
            
            result = st.session_state.analysis_result
            
            # Metadata
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Model", result['metadata']['model'])
            with col2:
                st.metric("Log Size", f"{result['metadata']['log_length']} chars")
            with col3:
                st.metric("Analyzed At", result['timestamp'])
            
            # Analysis content
            st.markdown("---")
            st.markdown("#### 🔍 AI Analysis")
            
            analysis_text = result.get('analysis', 'No analysis available')
            
            # Display in a nice box
            st.markdown(
                f"""
                <div style='background-color: #1e1e1e; padding: 20px; border-radius: 10px; border-left: 5px solid #4CAF50;'>
                <pre style='color: #ffffff; white-space: pre-wrap; word-wrap: break-word; font-family: monospace; font-size: 14px;'>{analysis_text}</pre>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            # Metadata details
            with st.expander("📊 Analysis Metadata"):
                st.json(result['metadata'])
            
            # STEP 8: LLM Response Feedback
            st.markdown("---")
            st.markdown("### 💬 LLM Response Feedback")
            st.info("Help us improve our AI analysis by providing feedback on the results")
            
            with st.expander("📝 Provide Feedback", expanded=False):
                st.markdown("*Your feedback helps improve the accuracy of future analyses*")
                
                # Feedback key prefix
                feedback_key_prefix = f"feedback_{selected_txn_id}"
                
                # User authentication data
                users = {
                    "Select User": {"email": "", "passcode": ""},
                    "John Smith (john.smith@company.com)": {"email": "john.smith@company.com", "passcode": "1234"},
                    "Sarah Johnson (sarah.johnson@company.com)": {"email": "sarah.johnson@company.com", "passcode": "5678"},
                    "Michael Brown (michael.brown@company.com)": {"email": "michael.brown@company.com", "passcode": "9012"},
                    "Emily Davis (emily.davis@company.com)": {"email": "emily.davis@company.com", "passcode": "3456"},
                    "Robert Wilson (robert.wilson@company.com)": {"email": "robert.wilson@company.com", "passcode": "7890"}
                }
                
                # Question 1: Rating
                st.markdown("#### 1️⃣ Rate the Analysis Quality")
                rating = st.select_slider(
                    "How would you rate the accuracy and usefulness of the AI analysis?",
                    options=[0, 1, 2, 3, 4, 5],
                    value=3,
                    format_func=lambda x: f"{x} - {'Poor' if x <= 1 else 'Fair' if x <= 2 else 'Good' if x <= 3 else 'Very Good' if x <= 4 else 'Excellent'}",
                    key=f"{feedback_key_prefix}_rating"
                )
                
                # Question 2: Alternative Root Cause
                st.markdown("#### 2️⃣ Alternative Root Cause (if applicable)")
                
                anomaly_categories = [
                    "No alternative needed - AI analysis was correct",
                    "Customer Timeout/Abandonment",
                    "Customer Cancellation",
                    "Card Reading/Hardware Issues",
                    "PIN Authentication Problems",
                    "Cash Dispenser Malfunction",
                    "Account/Balance Issues",
                    "Network/Communication Errors",
                    "System/Software Errors",
                    "Receipt Printer Problems",
                    "Security/Fraud Detection",
                    "Database/Core Banking Issues",
                    "Environmental Factors (Power, etc.)",
                    "User Interface/Display Problems",
                    "Other (please specify in comments)"
                ]
                
                alternative_cause = st.selectbox(
                    "If you believe the AI identified the wrong root cause, please select the correct one:",
                    anomaly_categories,
                    key=f"{feedback_key_prefix}_alternative"
                )
                
                # Question 3: Comments
                st.markdown("#### 3️⃣ Additional Comments")
                feedback_comment = st.text_area(
                    "Please provide any specific feedback, suggestions, or observations:",
                    placeholder="e.g., 'The analysis missed...', 'Very helpful analysis!', 'Could improve by...'",
                    height=100,
                    key=f"{feedback_key_prefix}_comment"
                )
                
                # Check if any question has been answered
                rating_answered = st.session_state.get(f"{feedback_key_prefix}_rating", 3) != 3
                alternative_answered = st.session_state.get(f"{feedback_key_prefix}_alternative", anomaly_categories[0]) != anomaly_categories[0]
                comment_answered = bool(st.session_state.get(f"{feedback_key_prefix}_comment", "").strip())
                
                questions_answered = sum([rating_answered, alternative_answered, comment_answered])
                
                # User Authentication (only if questions answered)
                user_name = ""
                user_email = ""
                passcode_verified = False
                
                if questions_answered > 0:
                    st.markdown("---")
                    st.markdown("#### 👤 User Authentication Required")
                    st.info("Please identify yourself and enter your passcode to submit feedback.")
                    
                    selected_user = st.selectbox(
                        "Select your name and email:",
                        list(users.keys()),
                        key=f"{feedback_key_prefix}_user_select"
                    )
                    
                    if selected_user != "Select User":
                        user_name = selected_user.split(" (")[0]
                        user_email = users[selected_user]["email"]
                        
                        st.markdown("#### 🔐 Enter Your 4-Digit Passcode")
                        entered_passcode = st.text_input(
                            "Passcode:",
                            type="password",
                            max_chars=4,
                            key=f"{feedback_key_prefix}_passcode"
                        )
                        
                        if entered_passcode:
                            if len(entered_passcode) == 4 and entered_passcode.isdigit():
                                correct_passcode = users[selected_user]["passcode"]
                                if entered_passcode == correct_passcode:
                                    passcode_verified = True
                                    st.success("✅ Passcode verified! You can now submit feedback.")
                                else:
                                    st.error("❌ Incorrect passcode. Please try again.")
                            else:
                                st.warning("⚠️ Passcode must be exactly 4 digits.")
                    
                    if selected_user == "Select User":
                        st.warning("⚠️ Please select your name and email to continue.")
                    elif not passcode_verified and selected_user != "Select User":
                        st.warning("⚠️ Please enter your correct 4-digit passcode to submit feedback.")
                
                # Submit Feedback
                st.markdown("---")
                col1, col2, col3 = st.columns([2, 2, 3])
                
                with col1:
                    can_submit = questions_answered > 0 and selected_user != "Select User" and passcode_verified
                    
                    if st.button("Submit Feedback", 
                            key=f"{feedback_key_prefix}_submit",
                            disabled=not can_submit,
                            type="primary",
                            use_container_width=True):
                        
                        if questions_answered == 0:
                            st.error("Please answer at least one question before submitting.")
                        elif selected_user == "Select User":
                            st.error("Please select your name and email.")
                        elif not passcode_verified:
                            st.error("Please enter the correct passcode.")
                        else:
                            # Submit feedback to API
                            with st.spinner("Submitting feedback..."):
                                try:
                                    result = st.session_state.analysis_result
                                    
                                    feedback_data = {
                                        "transaction_id": selected_txn_id,
                                        "rating": st.session_state.get(f"{feedback_key_prefix}_rating", 3),
                                        "alternative_cause": st.session_state.get(f"{feedback_key_prefix}_alternative", anomaly_categories[0]),
                                        "comment": st.session_state.get(f"{feedback_key_prefix}_comment", ""),
                                        "user_name": user_name,
                                        "user_email": user_email,
                                        "model_version": result['metadata']['model'],
                                        "original_llm_response": result.get('analysis', '')
                                    }
                                    
                                    response = requests.post(
                                        f"{API_BASE_URL}/submit-llm-feedback",
                                        json=feedback_data,
                                        timeout=30
                                    )
                                    
                                    if response.status_code == 200:
                                        result_data = response.json()
                                        st.success(result_data['message'])
                                        
                                        # Clear form
                                        keys_to_clear = [
                                            f"{feedback_key_prefix}_rating",
                                            f"{feedback_key_prefix}_alternative",
                                            f"{feedback_key_prefix}_comment",
                                            f"{feedback_key_prefix}_user_select",
                                            f"{feedback_key_prefix}_passcode"
                                        ]
                                        for key in keys_to_clear:
                                            if key in st.session_state:
                                                del st.session_state[key]
                                        
                                        import time
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        error_detail = response.json().get('detail', 'Failed to submit')
                                        st.error(f"❌ {error_detail}")
                                        
                                except Exception as e:
                                    st.error(f"❌ Error submitting feedback: {str(e)}")

                with col2:
                    if st.button("Clear Form", 
                            key=f"{feedback_key_prefix}_clear",
                            use_container_width=True):
                        keys_to_clear = [
                            f"{feedback_key_prefix}_rating",
                            f"{feedback_key_prefix}_alternative",
                            f"{feedback_key_prefix}_comment",
                            f"{feedback_key_prefix}_user_select",
                            f"{feedback_key_prefix}_passcode"
                        ]
                        for key in keys_to_clear:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.rerun()

            # Display previous feedback
            try:
                feedback_response = requests.get(
                    f"{API_BASE_URL}/get-feedback/{selected_txn_id}",
                    timeout=30
                )
                
                if feedback_response.status_code == 200:
                    feedback_data = feedback_response.json()
                    previous_feedback = feedback_data.get('feedback', [])
                    
                    if previous_feedback:
                        with st.expander(f"📊 Previous Feedback ({len(previous_feedback)})", expanded=False):
                            for i, feedback in enumerate(previous_feedback, 1):
                                st.markdown(f"**Feedback #{i} - {feedback['timestamp']}**")
                                st.write(f"**Submitted by:** {feedback['user_name']} ({feedback['user_email']})")
                                st.write(f"**Model:** {feedback.get('model_version', 'Unknown')}")
                                st.write(f"**Rating:** {feedback['rating']}/5")
                                if feedback.get('alternative_cause') and feedback['alternative_cause'] != "No alternative needed - AI analysis was correct":
                                    st.write(f"**Alternative Cause:** {feedback['alternative_cause']}")
                                if feedback.get('comment'):
                                    st.write(f"**Comment:** {feedback['comment']}")
                                st.markdown("---")
            except:
                pass  # Silently fail if can't retrieve feedback
    
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        import traceback
        with st.expander("🐛 Debug Information"):
            st.code(traceback.format_exc())

# ============================================
# MAIN APPLICATION
# ============================================

st.title("DN Diagnostics Platform")
st.caption("Comprehensive analysis tool for Diebold Nixdorf diagnostic files.")

st.markdown("## Upload Zip Package")

uploaded_file = st.file_uploader(
    "Select ZIP Archive",
    type=['zip'],
    help="Upload a ZIP file containing diagnostic files (max 500 MB)",
    key="zip_uploader"
)

# Only process if file exists AND it's different from the last processed file
if uploaded_file is not None:
    # Check if this is a new file or the same file we just processed
    current_file_id = f"{uploaded_file.name}_{uploaded_file.size}"
    
    if 'last_processed_file' not in st.session_state:
        st.session_state.last_processed_file = None
    
    # Only process if it's a new file
    if st.session_state.last_processed_file != current_file_id:
        file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
        st.info(f"File: {uploaded_file.name} ({file_size_mb:.2f} MB)")
        
        with st.spinner("Processing package..."):
            try:
                files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "application/zip")}
                
                response = requests.post(
                    f"{API_BASE_URL}/process-zip", 
                    files=files,
                    timeout=120
                )
                
                if response.status_code == 200:
                    result = response.json()
                    st.session_state.zip_processed = True
                    st.session_state.processing_result = result
                    st.session_state.last_processed_file = current_file_id  # Store the file ID
                    
                    st.success("Package processed successfully.")
                    st.rerun()
                else:
                    error_detail = response.json().get('detail', 'Unknown error occurred.')
                    st.error(f"Error: {error_detail}")
                    
            except requests.exceptions.Timeout:
                st.error("Request timeout. The file may be too large or the server is not responding.")
            except requests.exceptions.ConnectionError:
                st.error("Connection error. Please ensure the FastAPI server is running on localhost:8000.")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    else:
        # File already processed, show info
        file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)

if st.session_state.zip_processed:
    st.markdown("---")
    result = st.session_state.processing_result
    categories = result['categories']
    
    st.markdown("## Detected Files")
    
    cols = st.columns(5)
    
    category_display = {
        'customer_journals': ('Customer Journals', '📋'),
        'ui_journals': ('UI Journals', '🖥️'),
        'trc_trace': ('TRC Trace', '📝'),
        'trc_error': ('TRC Error', '⚠️'),
        'registry_files': ('Registry Files', '📄')
    }
    
    for idx, (category, (label, icon)) in enumerate(category_display.items()):
        count = categories.get(category, {}).get('count', 0)
        with cols[idx]:
            st.metric(label, count)
    
    st.markdown("---")
    
    st.markdown("## Analysis Functions")

    functionalities = {
        "transaction_stats": {
            "name": "📊 Transaction Type Statistics",
            "description": "View statistics for different transaction types",
            "status": "ready",
            "requires": ["customer_journals"]
        },
        "individual_transaction": {
            "name": "🔍 Individual Transaction Analysis",
            "description": "Analyze a specific transaction in detail",
            "status": "ready",
            "requires": ["customer_journals"]
        },
        "ui_flow_individual": {
            "name": "🖥️ UI Flow of Individual Transaction",
            "description": "Visualize UI flow for a specific transaction",
            "status": "ready",
            "requires": ["customer_journals", "ui_journals"]
        },
        "consolidated_flow": {
            "name": "🌐 Consolidated Transaction UI Flow and Analysis",
            "description": "View consolidated flow across multiple transactions",
            "status": "ready",
            "requires": ["customer_journals", "ui_journals"]
        },
        "transaction_comparison": {
            "name": "⚖️ Transaction Comparison Analysis",
            "description": "Compare two transactions side by side",
            "status": "ready",
            "requires": ["customer_journals", "ui_journals"]
        },
        "registry_single": {
            "name": "📝 Single View of Registry Files",
            "description": "View and analyze a single registry file",
            "status": "ready",
            "requires": ["registry_files"]
        },
        "registry_compare": {
            "name": "🔄 Compare Two Registry Files",
            "description": "Compare differences between two registry files",
            "status": "ready",
            "requires": ["registry_files"]
        }
    }

    available_file_types = [cat for cat, data in categories.items() if data.get('count', 0) > 0]

    # Build dropdown options in the order defined in functionalities
    dropdown_options = ["Select a function"]

    for func_id, func_data in functionalities.items():
        requirements_met = all(req in available_file_types for req in func_data['requires'])
        
        if requirements_met:
            # Add the function name as-is
            dropdown_options.append(func_data['name'])
        else:
            # Add with missing requirements indicator
            missing = [req for req in func_data['requires'] if req not in available_file_types]
            req_labels = {
                'customer_journals': 'Customer Journals',
                'ui_journals': 'UI Journals',
                'trc_trace': 'TRC Trace',
                'trc_error': 'TRC Error',
                'registry_files': 'Registry Files'
            }
            missing_str = ", ".join([req_labels.get(m, m) for m in missing])
            dropdown_options.append(f"{func_data['name']} (Missing: {missing_str})")

    selected_option = st.selectbox(
        "Select Analysis Function",
        options=dropdown_options,
        key="function_selector"
    )

    selected_func_id = None
    selected_func_data = None

    if selected_option != "Select a function":
        clean_option = selected_option.split(" (Missing:")[0]
        
        for func_id, func_data in functionalities.items():
            if func_data['name'] == clean_option:
                selected_func_id = func_id
                selected_func_data = func_data
                break

    if selected_func_data:
        st.markdown("---")
        
        requirements_met = all(req in available_file_types for req in selected_func_data['requires'])
        
        if not requirements_met:
            missing = [req for req in selected_func_data['requires'] if req not in available_file_types]
            missing_str = ", ".join([req_labels.get(m, m) for m in missing])
            st.error(f"Cannot proceed. Missing required files: {missing_str}")
            st.info("Please upload a package containing the required file types.")
        
        elif selected_func_data['status'] == 'construction':
            render_under_construction(selected_func_data['name'])
        
        else:
            # Ready functionalities - show their content
            if selected_func_id == "transaction_stats":
                render_transaction_stats()           
            elif selected_func_id == "individual_transaction":
                render_individual_transaction_analysis()
            elif selected_func_id == "registry_single":
                render_registry_single()
            elif selected_func_id == "registry_compare":
                render_registry_compare()
            elif selected_func_id == "transaction_comparison":
                render_transaction_comparison()
            elif selected_func_id == "ui_flow_individual":
                render_ui_flow_individual()
            elif selected_func_id == "consolidated_flow":
                render_consolidated_flow()

st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #666666; font-size: 0.875rem;'>
        © 2025 Diebold Nixdorf Analysis Tools
    </div>
""", unsafe_allow_html=True)