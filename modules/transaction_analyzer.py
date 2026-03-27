"""
Transaction Analyzer Service - Parses and analyzes customer journal files
and optionally enriches transactions with UI journal (JRN) data.
"""

import pandas as pd
import re
import os
import json
from datetime import datetime, timedelta
from datetime import time as dt_time          # FIX 1: explicit alias avoids conflict with
                                               # Python's built-in `time` module
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from modules.configManager import xml_to_dict
from modules.logging_config import logger

# Import log preprocessor and merger from processing module
from modules.processing import LogPreprocessorService, TransactionMergerService

# Central JOURNAL folder parser — matching, parsing, and diagnostic extraction
from modules.journal_parser import (
    match_journal_file,
    parse_journal,
    extract_diagnostic_context,
)


logger.info("Transaction Analyzer Service loaded")


# ─────────────────────────────────────────────────────────────
#  JRN fields to enrich into the EJ DataFrame as new columns.
#  Each key is the JRN dict key; value is the DataFrame column name.
# ─────────────────────────────────────────────────────────────
_JRN_ENRICH_MAP = {
    "protocol_steps":      "JRN Protocol Steps",
    "device_errors":       "JRN Device Errors",
    "response_code":       "JRN Response Code",
    "stan":                "JRN STAN",
    "retract_counter":     "JRN Retract Counter",
    "card_ejected":        "JRN Card Ejected",
    "app_state_start":     "JRN App State Start",
    "app_state_end":       "JRN App State End",
    "account_preference":  "JRN Account Preference",
    "preferences_restored":"JRN Preferences Restored",
    "present_timeout":     "JRN Present Timeout",
    "events":              "JRN Events",
    # Fields populated by extract_diagnostic_context (journal_parser.py)
    "device_states":       "JRN Device States",
    "card_events":         "JRN Card Events",
}

# All JRN column names as a set — used for enrichment count checks
_JRN_COLUMNS = set(_JRN_ENRICH_MAP.values())


def _safe_ts(val) -> Optional[str]:
    """
    FIX 2: Safely convert a Start Time / End Time value to HH:MM:SS string.
    Handles: dt_time objects, strings, NaT, NaN, None — never returns "NaT".
    """
    if val is None:
        return None
    # pandas NaT / NaN
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, dt_time):
        return val.strftime("%H:%M:%S")
    s = str(val).strip()
    # Reject pandas sentinel strings
    if s in ("NaT", "NaN", "nan", "None", ""):
        return None
    # Validate it looks like HH:MM:SS
    if re.match(r'^\d{2}:\d{2}:\d{2}', s):
        return s[:8]
    return None


class TransactionAnalyzerService:
    """
    CLASS: TransactionAnalyzerService

    DESCRIPTION:
        Service class for parsing and analyzing customer journal log files.
        Loads XML configuration and provides methods to extract transactions,
        detect flow patterns, compare transactions and generate analysis output.
        Optionally enriches transaction data with UI journal (JRN) fields via
        LogPreprocessorService and TransactionMergerService.

    USAGE:
        analyzer = TransactionAnalyzerService()

        # Customer journal only (original behaviour)
        result = analyzer.analyze_customer_journals(list_of_ej_files)

        # With UI journal enrichment
        result = analyzer.analyze_customer_journals(
            list_of_ej_files,
            ui_journal_files=list_of_jrn_files
        )

    ATTRIBUTES:
        real_dict (dict) : Mapping of raw function names to readable names
        start_key (list) : List of TIDs representing transaction start
        end_key (list)   : List of TIDs representing transaction end
        chain_key (list) : List of TIDs used for transaction chaining

    RAISES:
        FileNotFoundError : When dnLogAtConfig.xml file is not found
    """

    def __init__(self):
        """
        FUNCTION: __init__

        DESCRIPTION:
            Initializes the analyzer, loads XML configuration, resolves paths
            and prepares key mappings (start, end, chaining TIDs).
            Also initialises LogPreprocessorService and TransactionMergerService
            for optional UI journal enrichment.

        USAGE:
            analyzer = TransactionAnalyzerService()

        PARAMETERS:
            None

        RETURNS:
            None

        RAISES:
            FileNotFoundError : If XML configuration is missing
        """
        base_dir = Path(__file__).resolve().parent
        possible_paths = [
            base_dir / 'config' / 'dnLogAtConfig.xml',
            base_dir.parent / 'config' / 'dnLogAtConfig.xml',
            base_dir / 'dnLogAtConfig.xml',
        ]

        config_path = None
        for path in possible_paths:
            if path.exists():
                config_path = path
                break

        if config_path is None:
            logger.error(
                "dnLogAtConfig.xml not found. Searched in:\n%s",
                "\n".join(map(str, possible_paths))
            )
            raise FileNotFoundError(
                "dnLogAtConfig.xml not found. Please ensure the config file exists. Searched in:\n" +
                "\n".join(map(str, possible_paths))
            )

        self.real_dict, self.start_key, self.end_key, self.chain_key = xml_to_dict(config_path)
        logger.info("Configuration loaded successfully from %s", config_path)

        # Services for UI journal processing
        self._log_preprocessor = LogPreprocessorService()
        self._merger            = TransactionMergerService()

    # ============================================
    # PRIVATE — UI JOURNAL HELPERS
    # ============================================

    def _load_jrn_records(self, ui_journal_files: List[str]) -> List[dict]:
        """
        FUNCTION: _load_jrn_records

        DESCRIPTION:
            Reads and preprocesses all provided UI journal files into a flat
            list of JRN transaction records using LogPreprocessorService.

        USAGE:
            jrn_records = self._load_jrn_records(["ui/20250916.jrn"])

        PARAMETERS:
            ui_journal_files (List[str]) :
                Full paths to UI journal (.jrn) files.

        RETURNS:
            List[dict] : Flat list of parsed JRN transaction records.

        RAISES:
            None (errors logged internally per file)
        """
        all_jrn = []
        for jrn_file in ui_journal_files:
            try:
                raw = Path(jrn_file).read_text(encoding="utf-8", errors="replace")

                # FIX 3: Log first 3 lines so format mismatches are immediately visible
                preview = raw.splitlines()[:3]
                logger.debug("JRN file preview (%s): %s", Path(jrn_file).name, preview)

                records = self._log_preprocessor.preprocess_jrn(raw)
                all_jrn.extend(records)
                logger.info("Parsed %d JRN records from %s", len(records), Path(jrn_file).name)

                # FIX 3: Warn explicitly when 0 records — signals a format mismatch
                if not records:
                    logger.warning(
                        "preprocess_jrn returned 0 records for %s — "
                        "check that log lines match HH:MM:SS CODE message format",
                        Path(jrn_file).name
                    )
                else:
                    logger.debug(
                        "JRN sample record keys: %s | ts_start: %s",
                        list(records[0].keys()),
                        records[0].get("ts_start")
                    )

            except Exception as e:
                logger.error("Failed to parse JRN file %s: %s", jrn_file, e, exc_info=True)
        return all_jrn

    def _build_ej_records_from_df(self, df: pd.DataFrame) -> List[dict]:
        """
        FUNCTION: _build_ej_records_from_df

        DESCRIPTION:
            Converts the EJ transaction DataFrame produced by
            parse_customer_journal into the minimal dict format expected by
            TransactionMergerService (needs ts_start, ts_end, txn_number).

        USAGE:
            ej_records = self._build_ej_records_from_df(df)

        PARAMETERS:
            df (pd.DataFrame) :
                DataFrame with columns: Transaction ID, Start Time, End Time.

        RETURNS:
            List[dict] : Light-weight EJ records compatible with the merger.

        RAISES:
            None
        """
        records = []
        for _, row in df.iterrows():
            # FIX 2: use _safe_ts — handles NaT/NaN/string variants, never returns "NaT"
            ts_start = _safe_ts(row.get("Start Time"))
            ts_end   = _safe_ts(row.get("End Time"))
            records.append({
                "txn_number": row.get("Transaction ID"),
                "ts_start":   ts_start,
                "ts_end":     ts_end,
            })

        # FIX 3: Log sample so timestamp issues are visible immediately
        if records:
            logger.debug(
                "EJ records sample — txn: %s | ts_start: %s | ts_end: %s",
                records[0].get("txn_number"),
                records[0].get("ts_start"),
                records[0].get("ts_end"),
            )
        null_ts = sum(1 for r in records if r["ts_start"] is None)
        if null_ts:
            logger.warning(
                "%d / %d EJ records have null ts_start — "
                "these will not match JRN by timestamp",
                null_ts, len(records)
            )
        return records

    def _enrich_df_with_jrn(
        self,
        df: pd.DataFrame,
        jrn_records: List[dict],
        ui_journal_files: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        FUNCTION: _enrich_df_with_jrn

        DESCRIPTION:
            Merges JRN records into the EJ DataFrame by matching on
            Transaction ID / UUID (tier 1) and timestamp overlap (tier 2).
            Adds new columns for every field defined in _JRN_ENRICH_MAP.

            Additionally calls extract_diagnostic_context (journal_parser.py)
            per transaction to populate the previously missing fields:
                - JRN Device States  (5011 device state changes)
                - JRN Card Events    (3205/3245/3973/5135 card events)
            and to fill/override:
                - JRN Protocol Steps (TDR_ transitions — richer raw scan)
                - JRN App State Start / JRN App State End
                - JRN Device Errors  (ErrorNr with class/code)

        PARAMETERS:
            df               (pd.DataFrame)        : EJ transaction DataFrame.
            jrn_records      (List[dict])           : JRN records from _load_jrn_records.
            ui_journal_files (Optional[List[str]])  : JOURNAL file paths for
                                                      diagnostic extraction.

        RETURNS:
            pd.DataFrame : Original DataFrame with additional JRN columns.

        RAISES:
            None (errors logged internally)
        """
        if df.empty or not jrn_records:
            logger.warning("Skipping JRN enrichment — empty DataFrame or no JRN records.")
            return df

        # Build minimal EJ dicts for the merger
        ej_records = self._build_ej_records_from_df(df)

        # Run the two-tier merge
        merged = self._merger.merge(ej_records, jrn_records)
        logger.info(
            "Merger produced %d records from %d EJ + %d JRN",
            len(merged), len(ej_records), len(jrn_records)
        )

        # Build a lookup: txn_number -> merged record
        jrn_lookup: Dict[str, dict] = {}
        for record in merged:
            txn_num = record.get("txn_number")
            if txn_num:
                jrn_lookup[txn_num] = record

        logger.debug(
            "JRN lookup built with %d entries: %s",
            len(jrn_lookup), list(jrn_lookup.keys())[:5]
        )

        # Initialise all JRN columns to None
        for col in _JRN_ENRICH_MAP.values():
            df[col] = None

        # Fill in JRN fields row by row from merger lookup
        matched_count = 0
        for idx, row in df.iterrows():
            txn_id  = row.get("Transaction ID")
            matched = jrn_lookup.get(txn_id)
            if matched is None:
                logger.debug("No JRN match for transaction %s", txn_id)
                continue
            matched_count += 1
            for jrn_key, col_name in _JRN_ENRICH_MAP.items():
                # Skip the new diagnostic fields — filled below
                if jrn_key in ("device_states", "card_events"):
                    continue
                val = matched.get(jrn_key)
                if val is not None:
                    df.at[idx, col_name] = str(val) if isinstance(val, (list, dict)) else val

        logger.info(
            "JRN lookup matched %d / %d EJ transactions",
            matched_count, len(df)
        )

        # ── Diagnostic context via journal_parser.extract_diagnostic_context ─
        # Runs a direct raw-line scan of the JOURNAL file per transaction,
        # capturing ErrorNr, TDR steps, device states, card events, app state.
        if ui_journal_files:
            diag_count = 0
            for idx, row in df.iterrows():
                txn_id     = row.get("Transaction ID")
                start_time = _safe_ts(row.get("Start Time"))
                end_time   = _safe_ts(row.get("End Time"))

                if not start_time or not end_time:
                    logger.debug(
                        "Skipping diagnostic context for %s — missing timestamps",
                        txn_id
                    )
                    continue

                # Find the matching JOURNAL file for this transaction
                # (keyed by the EJ source file stem, e.g. "20250909")
                source_stem  = str(row.get("Source_File", ""))
                matched_file = None
                for jf in ui_journal_files:
                    if Path(jf).stem == source_stem:
                        matched_file = jf
                        break
                # Fall back to first available if no exact match
                if matched_file is None and ui_journal_files:
                    matched_file = ui_journal_files[0]

                if matched_file is None:
                    continue

                ctx = extract_diagnostic_context(matched_file, start_time, end_time)

                # Always populate device_states and card_events from raw scan
                if ctx["device_states"]:
                    df.at[idx, "JRN Device States"] = str(ctx["device_states"])
                if ctx["card_events"]:
                    df.at[idx, "JRN Card Events"] = str(ctx["card_events"])

                # Fill protocol_steps / device_errors / app_state only when
                # merger left them empty (raw scan is more reliable for these)
                if not df.at[idx, "JRN Protocol Steps"] and ctx["protocol_steps"]:
                    df.at[idx, "JRN Protocol Steps"] = str(ctx["protocol_steps"])
                if not df.at[idx, "JRN Device Errors"] and ctx["device_errors"]:
                    df.at[idx, "JRN Device Errors"] = str(ctx["device_errors"])
                if not df.at[idx, "JRN App State Start"] and ctx["app_state_start"]:
                    df.at[idx, "JRN App State Start"] = ctx["app_state_start"]
                if not df.at[idx, "JRN App State End"] and ctx["app_state_end"]:
                    df.at[idx, "JRN App State End"] = ctx["app_state_end"]

                diag_count += 1

            logger.info(
                "Diagnostic context extracted for %d / %d transactions",
                diag_count, len(df)
            )

        # Count enrichment across ALL JRN columns
        jrn_cols       = list(_JRN_ENRICH_MAP.values())
        enriched_count = int(df[jrn_cols].notna().any(axis=1).sum())
        logger.info(
            "JRN enrichment complete: %d / %d transactions enriched",
            enriched_count, len(df)
        )
        return df


    def analyze_customer_journals(
        self,
        customer_journal_files: List[str],
        ui_journal_files: Optional[List[str]] = None
    ) -> Dict:
        """
        FUNCTION: analyze_customer_journals

        DESCRIPTION:
            Processes multiple customer journal text files, extracts transaction
            data from each file, merges them, and generates summary statistics.
            If ui_journal_files are provided, each transaction is additionally
            enriched with fields from the matching UI journal record (protocol
            steps, device errors, response code, STAN, retract counter, etc.).

        USAGE:
            # EJ only (original behaviour — fully backward compatible)
            result = analyzer.analyze_customer_journals(["file1.jrn"])

            # With UI journal enrichment
            result = analyzer.analyze_customer_journals(
                customer_journal_files=["customer/20250916.jrn"],
                ui_journal_files=["ui/20250916.jrn"]
            )

        PARAMETERS:
            customer_journal_files (List[str]) :
                List of full file paths for customer (EJ) journals.
            ui_journal_files (Optional[List[str]]) :
                List of full file paths for UI (JRN) journals.
                Pass None or omit to skip enrichment (default behaviour).

        RETURNS:
            dict : {
                "transactions": [ ... list of txn dictionaries ... ],
                "summary": {
                    "total_transactions": int,
                    "successful": int,
                    "unsuccessful": int,
                    "transaction_types": list,
                    "jrn_enriched": int   # present only when ui_journal_files supplied
                }
            }

        RAISES:
            Exception : Logged internally when file parsing fails
        """
        # FIX 3: Load JRN records ONCE outside the per-file loop.
        # Original code called _load_jrn_records inside the loop, re-parsing
        # the same UI journal files for every EJ file — wasteful and error-prone.
        jrn_records_cache: List[dict] = []
        if ui_journal_files:
            jrn_records_cache = self._load_jrn_records(ui_journal_files)
            logger.info(
                "=== JRN DEBUG === files: %s | records parsed: %d",
                [Path(f).name for f in ui_journal_files],
                len(jrn_records_cache)
            )

        all_transactions = []

        for journal_file in customer_journal_files:
            try:
                logger.info("Processing: %s", Path(journal_file).name)

                df = self.parse_customer_journal(journal_file)

                if df is None or df.empty:
                    logger.warning("No data from %s", Path(journal_file).name)
                    continue

                # ── UI Journal enrichment ─────────────────────────────────
                if jrn_records_cache:
                    df = self._enrich_df_with_jrn(df, jrn_records_cache, ui_journal_files=ui_journal_files)
                # ─────────────────────────────────────────────────────────

                transactions = df.to_dict('records')

                # FIX 5: Safely serialise non-JSON-safe values
                for txn in transactions:
                    for key, value in list(txn.items()):
                        if not isinstance(value, (list, dict, str, bool)):
                            try:
                                if pd.isna(value):
                                    txn[key] = None
                                    continue
                            except (TypeError, ValueError):
                                pass
                        if hasattr(value, 'strftime'):
                            txn[key] = value.strftime('%H:%M:%S')
                        elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                            txn[key] = str(value)

                all_transactions.extend(transactions)
                logger.info(
                    "Found %d transactions in %s",
                    len(transactions), Path(journal_file).name
                )

            except Exception as e:
                logger.error(
                    "Error processing %s: %s",
                    Path(journal_file).name, e, exc_info=True
                )
                continue

        if not all_transactions:
            logger.warning("No transactions found in any journal files")
            return {
                "transactions": [],
                "summary": {
                    "total_transactions": 0,
                    "successful":         0,
                    "unsuccessful":       0,
                    "transaction_types":  []
                }
            }

        df_all = pd.DataFrame(all_transactions)

        summary = {
            "total_transactions": len(all_transactions),
            "successful":         0,
            "unsuccessful":       0,
            "transaction_types":  []
        }

        if 'End State' in df_all.columns:
            summary["successful"]   = len(df_all[df_all['End State'] == 'Successful'])
            summary["unsuccessful"] = len(df_all[df_all['End State'] == 'Unsuccessful'])

        if 'Transaction Type' in df_all.columns:
            summary["transaction_types"] = df_all['Transaction Type'].dropna().unique().tolist()

        # FIX 4: Count enrichment across ALL JRN columns, not just Response Code
        if ui_journal_files:
            present_jrn_cols = [c for c in _JRN_COLUMNS if c in df_all.columns]
            summary["jrn_enriched"] = (
                int(df_all[present_jrn_cols].notna().any(axis=1).sum())
                if present_jrn_cols else 0
            )
            logger.info(
                "JRN enriched transactions in summary: %d",
                summary["jrn_enriched"]
            )

        logger.info("Analysis complete: %d total transactions", summary['total_transactions'])
        logger.info("Successful: %d, Unsuccessful: %d", summary['successful'], summary['unsuccessful'])

        return {
            "transactions": all_transactions,
            "summary":      summary
        }

    def _map_transaction_type(self, raw_func: str) -> str:
        """
        Map raw function string to final transaction type.

        Logic:
        - Extract prefix before '/' (e.g., 'COUT/GA' -> 'COUT')
        - Look up ONLY the prefix in config (self.real_dict)
        - If not found, return the prefix itself
        """
        if not raw_func:
            return raw_func

        prefix   = raw_func.split('/')[0].strip()
        txn_type = self.real_dict.get(prefix, prefix)

        logger.debug(
            "Transaction type mapped: raw='%s' -> prefix='%s' -> type='%s'",
            raw_func, prefix, txn_type
        )

        return txn_type

    # ============================================
    # ALL EXISTING METHODS BELOW (UNCHANGED)
    # ============================================

    def parse_customer_journal(self, file_path: str) -> pd.DataFrame:
        """
        FUNCTION: parse_customer_journal

        DESCRIPTION:
            Reads a customer journal log text file, extracts timestamp/TID/message
            information, and identifies transactions using internal logic.

        USAGE:
            df = parse_customer_journal("path/to/journal.txt")

        PARAMETERS:
            file_path (str) :
                Full path to a customer journal file.

        RETURNS:
            pd.DataFrame :
                DataFrame containing extracted transaction rows.

        RAISES:
            FileNotFoundError : When file cannot be read
            ValueError        : When parsing timestamp fails
        """
        logger.info("Parsing journal file: %s", file_path)
        dummy = Path(file_path).stem

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            logger.error("Failed to read file %s: %s", file_path, e, exc_info=True)
            return pd.DataFrame()

        parsed_rows = []
        for line in lines:
            line = line.strip()
            if not line or set(line) <= {'*'}:
                continue
            match = re.match(r"^(\d{2}:\d{2}:\d{2})\s+(\d+)\s*(.*)", line)
            if match:
                timestamp_str, tid, message = match.groups()
                try:
                    timestamp = datetime.strptime(timestamp_str, "%H:%M:%S").time()
                except ValueError:
                    timestamp = None
                parsed_rows.append([timestamp, tid, message])
            else:
                parsed_rows.append([None, None, line])

        df = pd.DataFrame(parsed_rows, columns=["timestamp", "tid", "message"])

        transactions = self._find_all_transactions(df, dummy)

        logger.debug("Parsed %d transactions from %s", len(transactions), file_path)
        return pd.DataFrame(transactions)

    def _find_all_transactions(self, df: pd.DataFrame, dummy: str) -> List[Dict]:
        """
        FUNCTION: _find_all_transactions

        DESCRIPTION:
            Internal method to identify all transaction boundaries based on
            start TIDs, end TIDs, and chained TIDs. Extracts each transaction's
            type, state, duration and full log.

        USAGE:
            txns = _find_all_transactions(df, "file1")

        PARAMETERS:
            df (pd.DataFrame) :
                Parsed journal rows (timestamp, tid, message)
            dummy (str) :
                File name stem used as fallback Transaction ID

        RETURNS:
            list of dict :
                Each dict contains:
                - Transaction ID
                - Start Time
                - End Time
                - Duration
                - Transaction Type
                - End State
                - Transaction Log
                - Source_File

        RAISES:
            None (all errors logged internally)
        """
        logger.info("Finding all transactions in parsed data for %s", dummy)
        transactions_bounds = []
        i = 0

        while i < len(df):
            row = df.iloc[i]

            if (str(row["tid"]) in [str(tid) for tid in self.start_key] or
                    str(row["tid"]) in [str(tid) for tid in self.chain_key]):
                start_idx = i
                j = i + 1
                end_idx = None

                while j < len(df):
                    current_row = df.iloc[j]

                    if str(current_row["tid"]) in [str(tid) for tid in self.end_key]:
                        end_idx = j
                        break

                    if ((str(current_row["tid"]) in [str(tid) for tid in self.start_key] or
                         str(current_row["tid"]) in [str(tid) for tid in self.chain_key]) and j > i + 3):
                        break

                    j += 1

                if end_idx is not None:
                    transactions_bounds.append((start_idx, end_idx))
                    i = end_idx + 1
                else:
                    i += 1
            else:
                i += 1

        transactions = []

        for start_idx, end_idx in transactions_bounds:
            txn_segment       = df.iloc[start_idx:end_idx + 1]
            start_time        = None
            txn_id            = None
            matched_start_tid = None

            for start_tid in self.start_key:
                start_matches = txn_segment[txn_segment["tid"] == str(start_tid)]
                if not start_matches.empty:
                    start_row  = start_matches.iloc[0]
                    start_time = start_row["timestamp"]
                    match = re.search(r"Transaction no\. '([^']*)'", start_row["message"])

                    # Always use filenameHHMMSS format (e.g. "20250909182726")
                    # regardless of whether the log contains a transaction number.
                    # This guarantees unique, traceable IDs across all files.
                    txn_id = dummy + start_time.strftime("%H%M%S")
                    logger.debug("txn_id assigned: %s", txn_id)

                    # Log the original transaction number for debugging only
                    if match and match.group(1).strip():
                        logger.debug(
                            "Original txn_number from log: %s (not used as ID)",
                            match.group(1).strip()
                        )
                    else:
                        for _, seg_row in txn_segment.iterrows():
                            seg_match = re.search(r"Transaction no\. '([^']+)'", seg_row["message"])
                            if seg_match and seg_match.group(1).strip():
                                logger.debug(
                                    "Original txn_number from segment (tid=%s): %s (not used as ID)",
                                    seg_row["tid"], seg_match.group(1).strip()
                                )
                                break

                    matched_start_tid = start_tid
                    break

            if start_time is None:
                for chain_tid in self.chain_key:
                    chain_matches = txn_segment[txn_segment["tid"] == str(chain_tid)]
                    if not chain_matches.empty:
                        start_row  = chain_matches.iloc[0]
                        start_time = start_row["timestamp"]

                        # Always use filenameHHMMSS format for chain transactions too
                        txn_id = dummy + start_time.strftime("%H%M%S") if start_time else f"CHAIN_{dummy}"
                        logger.debug("chain txn_id assigned: %s", txn_id)

                        # Log original number for debugging only
                        for _, seg_row in txn_segment.iterrows():
                            seg_match = re.search(r"Transaction no\. '([^']+)'", seg_row["message"])
                            if seg_match and seg_match.group(1).strip():
                                logger.debug(
                                    "Original chain txn_number (tid=%s): %s (not used as ID)",
                                    seg_row["tid"], seg_match.group(1).strip()
                                )
                                break

                        matched_start_tid = chain_tid
                        break

            end_time  = None
            end_state = "Unknown"

            for end_tid in self.end_key:
                end_matches = txn_segment[txn_segment["tid"] == str(end_tid)]
                if not end_matches.empty:
                    end_row  = end_matches.iloc[-1]
                    end_time = end_row["timestamp"]
                    end_msg  = end_row["message"]

                    if ("end-state'N'" in end_msg or "end-state'n'" in end_msg or
                            "state 'N'" in end_msg or "state 'n'" in end_msg):
                        end_state = "Successful"
                    elif ("end-state'E'" in end_msg or "end-state'e'" in end_msg or
                          "state 'E'" in end_msg or "state 'e'" in end_msg or
                          "state 'C'" in end_msg or "state 'c'" in end_msg):
                        end_state = "Unsuccessful"
                    else:
                        end_state = "Unknown"
                    break

            txn_type     = "Unknown"
            func_matches = txn_segment[txn_segment["tid"] == "3217"]
            if not func_matches.empty:
                for _, func_row in func_matches.iterrows():
                    func_match = re.search(r"Function\s+'([^']+)'", func_row["message"])
                    if func_match:
                        raw_func = func_match.group(1).strip()
                        txn_type = self._map_transaction_type(raw_func)
                        logger.info(f"Transaction type: raw='{raw_func}' -> type='{txn_type}'")
                        break

            txn_log_lines = []
            for _, row in txn_segment.iterrows():
                ts      = row["timestamp"].strftime("%H:%M:%S") if row["timestamp"] else "??:??:??"
                tid_val = row["tid"] if row["tid"] else ""
                msg_val = row["message"]
                txn_log_lines.append(f"{ts} {tid_val} {msg_val}")
            txn_log = "\n".join(txn_log_lines)

            duration_seconds = 0
            if start_time and end_time:
                try:
                    start_dt         = datetime.combine(datetime.today(), start_time)
                    end_dt           = datetime.combine(datetime.today(), end_time)
                    duration_seconds = (end_dt - start_dt).total_seconds()
                except Exception as e:
                    logger.warning("Duration calculation failed for txn %s: %s", txn_id, e)
                    duration_seconds = 0

            transactions.append({
                "Transaction ID":     txn_id,
                "Start Time":         start_time,
                "End Time":           end_time,
                "Duration (seconds)": duration_seconds,
                "Transaction Type":   txn_type,
                "End State":          end_state,
                "Transaction Log":    txn_log,
                "Source_File":        dummy
            })

        logger.debug("Found %d transactions for %s", len(transactions), dummy)
        return transactions

    def extract_actual_flows_from_txt_file(self, txt_file_path: str, selected_transaction_type: str) -> dict:
        """
        FUNCTION: extract_actual_flows_from_txt_file

        DESCRIPTION:
            Reads a pre-generated transaction_flows.txt file and extracts screen
            flow sequences for a given transaction type.

        USAGE:
            flows = extract_actual_flows_from_txt_file("transaction_flows.txt", "Withdrawal")

        PARAMETERS:
            txt_file_path (str) :
                Path to the flow file.
            selected_transaction_type (str) :
                Transaction type to filter flows.

        RETURNS:
            dict :
                {
                    "TXN_ID_1": { "screens": [...], "timestamp": "" },
                    "TXN_ID_2": { ... }
                }

        RAISES:
            None (all errors logged internally)
        """
        logger.info(
            "Extracting actual flows from %s for type '%s'",
            txt_file_path, selected_transaction_type
        )
        flows = {}

        if not os.path.exists(txt_file_path):
            logger.warning("File not found: %s", txt_file_path)
            return flows

        try:
            with open(txt_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            logger.error("Failed to read txt file %s: %s", txt_file_path, e, exc_info=True)
            return flows

        blocks = content.split('-' * 60)

        for block in blocks:
            if not block.strip():
                continue

            lines     = block.strip().split('\n')
            txn_id    = None
            txn_type  = None
            flow_line = None

            for line in lines:
                if line.startswith('Transaction ID:'):
                    txn_id = line.split(':', 1)[1].strip()
                elif line.startswith('Transaction Type:'):
                    txn_type = line.split(':', 1)[1].strip()
                elif line.startswith('Flow:'):
                    flow_line = line.split(':', 1)[1].strip()

            if txn_type == selected_transaction_type and txn_id and flow_line:
                if flow_line == 'No screen data available':
                    flows[txn_id] = {'screens': ['No flow data'], 'timestamp': ''}
                else:
                    screens = []
                    for part in flow_line.split('--'):
                        part = part.strip()
                        if '[' in part and ']' in part:
                            screen = part.split('[')[0].strip()
                            if screen:
                                screens.append(screen)
                    flows[txn_id] = {
                        'screens': screens if screens else ['No flow data'],
                        'timestamp': ''
                    }

        logger.info("Extracted %d flows for type '%s'", len(flows), selected_transaction_type)
        return flows

    def create_side_by_side_flow_comparison_data(
        self, df: pd.DataFrame, txn1_id: str, txn2_id: str, txt_file_path: str
    ) -> dict:
        """
        FUNCTION: create_side_by_side_flow_comparison_data

        DESCRIPTION:
            Generates comparison data for two transactions using the LCS algorithm
            to find matching and non-matching screens.

        USAGE:
            data = create_side_by_side_flow_comparison_data(df, "TXN001", "TXN002", "flows.txt")

        PARAMETERS:
            df (pd.DataFrame) :
                Full transaction DataFrame
            txn1_id (str) :
                First transaction ID
            txn2_id (str) :
                Second transaction ID
            txt_file_path (str) :
                Path to transaction_flows.txt

        RETURNS:
            dict : Comparison structure for UI rendering

        RAISES:
            None
        """
        txn1_data = df[df['Transaction ID'] == txn1_id].iloc[0]
        txn2_data = df[df['Transaction ID'] == txn2_id].iloc[0]

        transaction_type = txn1_data['Transaction Type']
        flows_data       = self.extract_actual_flows_from_txt_file(txt_file_path, transaction_type)

        txn1_flow = flows_data.get(txn1_id, {'screens': ['No flow data'], 'timestamp': ''})
        txn2_flow = flows_data.get(txn2_id, {'screens': ['No flow data'], 'timestamp': ''})

        def find_lcs_matches(flow1, flow2):
            """Find screens that appear in the same relative order in both flows using LCS"""
            m, n = len(flow1), len(flow2)
            lcs_table = [[0] * (n + 1) for _ in range(m + 1)]

            for i in range(1, m + 1):
                for j in range(1, n + 1):
                    if flow1[i - 1] == flow2[j - 1]:
                        lcs_table[i][j] = lcs_table[i - 1][j - 1] + 1
                    else:
                        lcs_table[i][j] = max(lcs_table[i - 1][j], lcs_table[i][j - 1])

            matches1 = [False] * m
            matches2 = [False] * n

            i, j = m, n
            while i > 0 and j > 0:
                if flow1[i - 1] == flow2[j - 1]:
                    matches1[i - 1] = True
                    matches2[j - 1] = True
                    i -= 1
                    j -= 1
                elif lcs_table[i - 1][j] > lcs_table[i][j - 1]:
                    i -= 1
                else:
                    j -= 1

            return matches1, matches2

        txn1_matches, txn2_matches = find_lcs_matches(txn1_flow['screens'], txn2_flow['screens'])

        return {
            'txn1_id':    txn1_id,
            'txn2_id':    txn2_id,
            'txn1_type':  str(txn1_data['Transaction Type']),
            'txn2_type':  str(txn2_data['Transaction Type']),
            'txn1_state': str(txn1_data['End State']),
            'txn2_state': str(txn2_data['End State']),
            'txn1_flow':  {'screens': txn1_flow['screens'], 'matches': txn1_matches},
            'txn2_flow':  {'screens': txn2_flow['screens'], 'matches': txn2_matches},
        }

    def generate_data_based_comparison_analysis(
        self, txn1_data, txn2_data, txn1_id: str, txn2_id: str, flows_data: dict
    ) -> str:
        """
        FUNCTION: generate_data_based_comparison_analysis

        DESCRIPTION:
            Generates detailed comparison analysis between two transactions using
            only actual data (no AI/LLM). Includes duration, steps, screens,
            source file info and uniqueness analysis.

        USAGE:
            analysis = generate_data_based_comparison_analysis(txn1, txn2, "ID1", "ID2", flows)

        PARAMETERS:
            txn1_data (dict or Series) : Data for transaction 1
            txn2_data (dict or Series) : Data for transaction 2
            txn1_id (str)              : Transaction ID 1
            txn2_id (str)              : Transaction ID 2
            flows_data (dict)          : Extracted flow mapping

        RETURNS:
            str : Formatted analysis output (markdown-style text)

        RAISES:
            Exception : Logged internally when errors occur
        """
        logger.info(f"Starting comparison analysis for transactions {txn1_id} and {txn2_id}")

        analysis     = [""]
        txn1_flow    = flows_data.get(txn1_id, {'screens': ['No flow data']})
        txn2_flow    = flows_data.get(txn2_id, {'screens': ['No flow data']})
        txn1_screens = txn1_flow['screens']
        txn2_screens = txn2_flow['screens']
        logger.debug("Transaction 1 screens: %s", txn1_screens)
        logger.debug("Transaction 2 screens: %s", txn2_screens)

        txn1_duration = None
        txn2_duration = None

        try:
            if txn1_data['Start Time'] and txn1_data['End Time']:
                # FIX 1: use dt_time alias throughout
                start_time    = txn1_data['Start Time'] if isinstance(txn1_data['Start Time'], dt_time) else datetime.strptime(str(txn1_data['Start Time']), '%H:%M:%S').time()
                end_time      = txn1_data['End Time']   if isinstance(txn1_data['End Time'],   dt_time) else datetime.strptime(str(txn1_data['End Time']),   '%H:%M:%S').time()
                txn1_duration = (datetime.combine(datetime.today(), end_time) -
                                 datetime.combine(datetime.today(), start_time)).total_seconds()
                logger.info("Transaction 1 duration calculated: %.1f seconds", txn1_duration)

            if txn2_data['Start Time'] and txn2_data['End Time']:
                start_time    = txn2_data['Start Time'] if isinstance(txn2_data['Start Time'], dt_time) else datetime.strptime(str(txn2_data['Start Time']), '%H:%M:%S').time()
                end_time      = txn2_data['End Time']   if isinstance(txn2_data['End Time'],   dt_time) else datetime.strptime(str(txn2_data['End Time']),   '%H:%M:%S').time()
                txn2_duration = (datetime.combine(datetime.today(), end_time) -
                                 datetime.combine(datetime.today(), start_time)).total_seconds()
                logger.info("Transaction 2 duration calculated: %.1f seconds", txn2_duration)

            if txn1_duration is not None and txn2_duration is not None:
                duration_diff = txn2_duration - txn1_duration
                logger.debug("Duration difference: %.1f seconds", duration_diff)
                analysis.append(f"**Actual Duration Data:**")
                analysis.append(f"   - Transaction 1 Duration: {txn1_duration:.1f} seconds")
                analysis.append(f"   - Transaction 2 Duration: {txn2_duration:.1f} seconds")
                if duration_diff > 0:
                    analysis.append(f"   - Transaction 2 took {duration_diff:.1f} seconds longer")
                elif duration_diff < 0:
                    analysis.append(f"   - Transaction 1 took {abs(duration_diff):.1f} seconds longer")
                else:
                    analysis.append(f"   - Both transactions took exactly the same time")
            else:
                logger.warning("One or both transaction durations could not be calculated")
                analysis.append(f"**⏱️ Duration Data:**")
                analysis.append(
                    f"   - Transaction 1 Duration: "
                    f"{f'{txn1_duration:.1f} seconds' if txn1_duration is not None else 'Cannot calculate (missing start/end time)'}"
                )
                analysis.append(
                    f"   - Transaction 2 Duration: "
                    f"{f'{txn2_duration:.1f} seconds' if txn2_duration is not None else 'Cannot calculate (missing start/end time)'}"
                )

        except Exception as e:
            logger.error("Error calculating durations: %s", e, exc_info=True)
            analysis.append(f"❌ Unable to calculate duration: {str(e)}")

        analysis.append("")
        analysis.append(f"**Actual Step Counts:**")
        analysis.append(f"   - Transaction 1 Steps: {len(txn1_screens)}")
        analysis.append(f"   - Transaction 2 Steps: {len(txn2_screens)}")

        step_diff = len(txn2_screens) - len(txn1_screens)
        logger.info(
            "Step count comparison: txn1=%d, txn2=%d, diff=%d",
            len(txn1_screens), len(txn2_screens), step_diff
        )

        if step_diff > 0:
            analysis.append(f"   - Transaction 2 has {step_diff} more steps")
        elif step_diff < 0:
            analysis.append(f"   - Transaction 1 has {abs(step_diff)} more steps")
        else:
            analysis.append(f"   - Both transactions have identical step counts")

        analysis.append("")

        if (len(txn1_screens) > 0 and len(txn2_screens) > 0
                and txn1_screens[0] != 'No flow data' and txn2_screens[0] != 'No flow data'):
            txn1_set       = set(txn1_screens)
            txn2_set       = set(txn2_screens)
            common_screens = txn1_set & txn2_set
            unique_txn1    = txn1_set - txn2_set
            unique_txn2    = txn2_set - txn1_set
            logger.info(
                "Screen comparison - common: %d, txn1 unique: %d, txn2 unique: %d",
                len(common_screens), len(unique_txn1), len(unique_txn2)
            )
            analysis.append(f"**Screen Usage Comparison:**")
            analysis.append(f"   - Common Screens: {len(common_screens)}")
            analysis.append(f"   - Transaction 1 Only: {len(unique_txn1)}")
            analysis.append(f"   - Transaction 2 Only: {len(unique_txn2)}")
            if unique_txn1:
                analysis.append(f"   - Transaction 1 Unique: {', '.join(sorted(unique_txn1))}")
            if unique_txn2:
                analysis.append(f"   - Transaction 2 Unique: {', '.join(sorted(unique_txn2))}")
            analysis.append(f"   - Total Unique Screens Used: {len(txn1_set | txn2_set)}")
        else:
            logger.warning("Insufficient flow data to analyze screens")
            analysis.append(f"**Screen Usage:** Cannot analyze - insufficient flow data")

        analysis.append("")
        analysis.append(f"**Data Source:**")
        if 'Source_File' in txn1_data.index:
            analysis.append(f"   - Transaction 1 Source: {txn1_data['Source_File']}")
        if 'Source_File' in txn2_data.index:
            analysis.append(f"   - Transaction 2 Source: {txn2_data['Source_File']}")

        if 'Source_File' in txn1_data.index and 'Source_File' in txn2_data.index:
            same_source = txn1_data['Source_File'] == txn2_data['Source_File']
            logger.info("Transactions come from the same source file: %s", same_source)
            analysis.append(f"   - Same Source File: {'Yes' if same_source else 'No'}")

        analysis.append("")
        logger.info("Comparison analysis completed for %s vs %s", txn1_id, txn2_id)
        return "\n".join(analysis)