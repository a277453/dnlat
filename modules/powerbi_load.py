import pandas as pd
import os

# ─────────────────────────────────────────────
# CONFIGURATION — must match paths in parse_journal.py
# ─────────────────────────────────────────────

STATS_CSV   = r"D:\python\DN\DNLoggingTool-main\stats_output.csv"    
DETAILS_CSV = r"D:\python\DN\DNLoggingTool-main\details_output.csv"  

# ─────────────────────────────────────────────

# Load both tables
stats_df   = pd.read_csv(STATS_CSV)
details_df = pd.read_csv(DETAILS_CSV)

# Ensure correct types for Power BI
for df in [stats_df, details_df]:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

# Power BI picks up any DataFrame defined at module level as a table.
# 'stats_df'   → appears as "stats_df" table in Power BI
# 'details_df' → appears as "details_df" table in Power BI

print("Loaded successfully!")
print(f"Stats   : {stats_df.shape[0]} rows, {stats_df.shape[1]} cols")
print(f"Details : {details_df.shape[0]} rows, {details_df.shape[1]} cols")
