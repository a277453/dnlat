# modules/chat_logger.py
"""
Chat conversation logger for DNLAT.

Appends each chat turn to a human-readable .txt file under chat_logs/.
One file per transaction, named by transaction_id and timestamp of first write.

File format:
───────────────────────────────────────────────────────
DNLAT Chat Log
Transaction : TXN-001
Session ID  : abc123
Started     : 2024-03-15 10:55:50
───────────────────────────────────────────────────────
[10:55:52]  USER
Why did this transaction fail?

[10:55:54]  ASSISTANT
The transaction failed due to a host timeout at step 3.

───────────────────────────────────────────────────────
Usage:
    from modules.chat_logger import ChatLogger

    logger = ChatLogger(transaction_id="TXN-001", session_id="abc123")
    logger.log_turn(role="user",      content="Why did it fail?")
    logger.log_turn(role="assistant", content="Host timeout.")
"""

import os
from datetime import datetime
from pathlib import Path

from modules.logging_config import logger

# Root directory for all chat log files.
# Reads DNLAT_CHAT_LOGS_DIR env var if set, otherwise falls back to
# a 'chat_logs' folder next to this file's parent (i.e. project root).
# Using an explicit env var avoids CWD ambiguity across platforms.
_env_override = os.getenv("DNLAT_CHAT_LOGS_DIR", "").strip()
if _env_override:
    _CHAT_LOGS_DIR = Path(_env_override)
else:
    # Path(__file__) is always the real absolute path of this .py file.
    # parent = modules/, parent.parent = project root
    _CHAT_LOGS_DIR = Path(__file__).resolve().parent.parent / "chat_logs"

logger.info("ChatLogger: chat_logs dir resolved to -> %s", _CHAT_LOGS_DIR)


def _ensure_logs_dir() -> Path:
    """Create chat_logs/ directory if it doesn't exist."""
    _CHAT_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return _CHAT_LOGS_DIR


def _safe_filename(transaction_id: str) -> str:
    """Sanitise transaction_id for use in a filename."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(transaction_id))


class ChatLogger:
    """
    Appends chat turns to a .txt file for a single transaction.

    Parameters
    ----------
    transaction_id : str
        Used in the filename and file header.
    session_id : str, optional
        Included in the file header for traceability.
    txn_data : dict, optional
        If provided, Start Time / End Time / Transaction Type are written
        into the file header on creation.
    """

    def __init__(
        self,
        transaction_id: str,
        session_id: str = "",
        username: str = "unknown",
        txn_data: dict = None,
    ):
        self.transaction_id = transaction_id
        self.session_id     = session_id
        self.username       = username or "unknown"
        self._file_path     = self._init_file(txn_data or {})

    # ── Public API ─────────────────────────────────────────────────────────────

    def log_turn(self, role: str, content: str) -> None:
        """
        Append a single turn to the log file.

        Parameters
        ----------
        role    : "user" or "assistant"
        content : The message text.
        """
        timestamp   = datetime.now().strftime("%H:%M:%S")
        role_label  = "USER" if role == "user" else "ASSISTANT"
        entry = f"[{timestamp}]  {role_label}\n{content.strip()}\n\n"

        try:
            with open(self._file_path, "a", encoding="utf-8") as f:
                f.write(entry)
        except OSError as exc:
            logger.warning(f"ChatLogger: failed to write turn to {self._file_path}: {exc}")

    @property
    def file_path(self) -> Path:
        """Return the resolved path to the log file."""
        return self._file_path

    # ── Internal ───────────────────────────────────────────────────────────────

    def _init_file(self, txn_data: dict) -> Path:
        """
        Create the log file and write the header.
        Returns the Path to the file.
        """
        logs_dir   = _ensure_logs_dir()
        safe_tid   = _safe_filename(self.transaction_id)
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_user  = _safe_filename(self.username)
        filename   = f"{safe_tid}_{safe_user}_{timestamp}.txt"
        file_path  = logs_dir / filename

        header_lines = [
            "─" * 60,
            "DNLAT Chat Log",
            f"Transaction : {self.transaction_id}",
            f"User        : {self.username}",
        ]
        if self.session_id:
            header_lines.append(f"Session ID  : {self.session_id}")

        header_lines.append(f"Started     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Optional txn metadata
        if txn_data.get("Transaction Type"):
            header_lines.append(f"Txn Type    : {txn_data['Transaction Type']}")
        if txn_data.get("Start Time"):
            header_lines.append(f"Start Time  : {txn_data['Start Time']}")
        if txn_data.get("End Time"):
            header_lines.append(f"End Time    : {txn_data['End Time']}")
        if txn_data.get("End State"):
            header_lines.append(f"End State   : {txn_data['End State']}")

        header_lines.append("─" * 60)
        header_lines.append("")  # blank line before first turn

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(header_lines))
            logger.info(f"ChatLogger: log file created -> {file_path}")
        except OSError as exc:
            logger.warning(f"ChatLogger: could not create log file {file_path}: {exc}")

        return file_path