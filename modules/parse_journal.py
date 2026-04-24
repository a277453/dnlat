import pandas as pd
import re
import statistics
import os
import configparser
from collections import defaultdict
from datetime import datetime

# ─────────────────────────────────────────────
# Load config.ini (must be in the same folder as this script)
# ─────────────────────────────────────────────

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")

config = configparser.ConfigParser()
config.optionxform = str          # (e.g. COUT, BAL)
config.read(CONFIG_FILE)

# Paths
JOURNAL_FILE_PATH = config["PATHS"]["journal_file"]
STATS_CSV         = config["PATHS"]["stats_csv"]
DETAILS_CSV       = config["PATHS"]["details_csv"]

# Parsing keys — same logic as original script:
#   start_key1 + start_key2 both present on the Function line → extract txn_type
#   end_key1 present on the finished line → extract end time
START_KEY1 = config["PARSING_KEYS"]["start_key1"]   # e.g. "3217"
START_KEY2 = config["PARSING_KEYS"]["start_key2"]   # e.g. "Function"
END_KEY1   = config["PARSING_KEYS"]["end_key1"]     # e.g. "3202"

# Transaction name mappings
TRANSACTION_NAME_MAP = dict(config["TRANSACTION_MAP"])

# ─────────────────────────────────────────────


def parse_cust_journal(input_txt_file):
    """
    Parses a customer journal .jrn file and returns two DataFrames:
      - stats_df   : summary statistics per transaction type
      - details_df : one row per transaction with start/end times and end state

    Parsing logic (mirrors original script):
      Pass 1 — split into blocks by *****,
               extract Transaction ID and End State per block.
      Pass 2 — for each block:
               - line with start_key1 AND start_key2 → start_time + txn_type
               - line with end_key1                  → end_time
    """
    with open(input_txt_file, "r") as f:
        content = f.read()

    blocks = re.split(r"\*{5,}", content)
    rows = []

    # ── Pass 1: extract Transaction ID and End State ──
    for block in blocks:
        block = block.strip()
        if not block:
            continue

        lines     = block.splitlines()
        txn_id    = None
        end_state = None
        end_tag   = None

        for line in lines:
            # Transaction ID — from started or finished line
            if not txn_id:
                m = re.search(r"Transaction no\. '(\w+)' (?:started|finished)", line)
                if m:
                    txn_id = m.group(1)

            # End state — from finished line e.g. end-state'E'
            if not end_state:
                m = re.search(r"end-state'([A-Z])'", line)
                if m:
                    end_state = m.group(1)
                    end_tag = {"N": "Successful", "E": "Unsuccessful"}.get(end_state, "Uncategorised")

        if txn_id:
            rows.append({
                "Timestamped Log": "\n".join(lines),
                "Transaction ID":  txn_id,
                "End State":       end_tag,
            })

    df_logs = pd.DataFrame(rows)

    # ── Pass 2: extract times and transaction type ──
    time_format       = "%H:%M:%S"
    durations_by_type = defaultdict(list)
    details_rows      = []

    for _, row in df_logs.iterrows():
        log       = row["Timestamped Log"]
        txn_id    = row["Transaction ID"]
        end_state = row["End State"]
        lines     = log.strip().split("\n")

        start_time = None
        end_time   = None
        txn_type   = None

        for line in lines:
            # Start line — must contain BOTH start_key1 and start_key2
            # e.g. "16:19:44  3217 Function 'VCPPREFS/PR' selected"
            if START_KEY1 in line and START_KEY2 in line:
                try:
                    start_time = datetime.strptime(line.strip().split()[0], time_format)
                    txn_type   = line.split("Function '")[1].split("/")[0]
                except (ValueError, IndexError):
                    pass

            # End line — must contain end_key1
            # e.g. "16:20:44  3202 Transaction no. 'xxx' finished ..."
            elif END_KEY1 in line:
                try:
                    end_time = datetime.strptime(line.strip().split()[0], time_format)
                except ValueError:
                    pass

        if start_time and end_time and txn_type:
            rl_name  = TRANSACTION_NAME_MAP.get(txn_type, txn_type)
            duration = (end_time - start_time).total_seconds()

            details_rows.append({
                "Transaction ID":   txn_id,
                "Transaction Type": rl_name,
                "Start Time":       start_time.strftime(time_format),
                "End Time":         end_time.strftime(time_format),
                "End State":        end_state,
                "Duration (s)":     duration,
            })

            durations_by_type[rl_name].append({
                "transaction_id":   txn_id,
                "start_time":       start_time.strftime(time_format),
                "end_time":         end_time.strftime(time_format),
                "duration_seconds": duration,
            })

    # ── Build statistics summary ──
    stats_rows = []
    for txn_type, entries in durations_by_type.items():
        max_entry = max(entries, key=lambda x: x["duration_seconds"])
        min_entry = min(entries, key=lambda x: x["duration_seconds"])
        avg_dur   = statistics.mean(e["duration_seconds"] for e in entries)

        stats_rows.append({
            "Transaction Type": txn_type,
            "Max Duration (s)": max_entry["duration_seconds"],
            "Max Start Time":   max_entry["start_time"],
            "Max End Time":     max_entry["end_time"],
            "Min Duration (s)": min_entry["duration_seconds"],
            "Min Start Time":   min_entry["start_time"],
            "Min End Time":     min_entry["end_time"],
            "Avg Duration (s)": round(avg_dur, 2),
            "Count":            len(entries),
        })

    stats_df   = pd.DataFrame(stats_rows)
    details_df = pd.DataFrame(details_rows)

    return stats_df, details_df


if __name__ == "__main__":
    print(f"Config loaded from : {CONFIG_FILE}")
    print(f"Reading journal    : {JOURNAL_FILE_PATH}")

    stats_df, details_df = parse_cust_journal(JOURNAL_FILE_PATH)

    stats_df.to_csv(STATS_CSV, index=False)
    details_df.to_csv(DETAILS_CSV, index=False)

    print(f"Stats saved        → {STATS_CSV}  ({len(stats_df)} rows)")
    print(f"Details saved      → {DETAILS_CSV}  ({len(details_df)} rows)")
    print("\nPreview — Details:")
    print(details_df.head())