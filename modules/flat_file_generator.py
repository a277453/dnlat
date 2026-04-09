"""
flat_file_merger.py
===================
Standalone module that merges Customer Journal and UI Journal .jrn files
into a single in-memory buffer per category.

Logic is identical to what was already working in routes.py — just moved here
so routes.py stays clean.  No session involvement.  No external storage.
Result lives in a StringIO buffer; optionally written to disk for verification.

HOW TO USE IN routes.py  (two lines only)
──────────────────────────────────────────
  # 1. Add to imports at top of routes.py:
  from modules.flat_file_merger import FlatFileMerger

  # 2. Call it right after categorization (file_categories is populated),
  #    before or after the session block — it does not touch the session:
  FlatFileMerger.run(
      customer_paths = file_categories.get('customer_journals', []),
      ui_paths       = file_categories.get('ui_journals', []),
  )
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import List, NamedTuple, Optional, Tuple

from modules.logging_config import logger


# ══════════════════════════════════════════════════════════════════════════════
#  RESULT  —  what the merger hands back for each category
# ══════════════════════════════════════════════════════════════════════════════

class MergeResult(NamedTuple):
    """
    Returned by FlatFileMerger.run() for each category.

    Fields
    ──────
    filename  : The computed output filename,
                e.g. 'customer_journal_11march25_to_04april25.jrn'
                None when no dated files were found.

    content   : Full merged text (StringIO buffer value).
                None when there was nothing to merge.

    file_count: Number of .jrn files that were merged.
    """
    filename   : Optional[str]
    content    : Optional[str]
    file_count : int


# ══════════════════════════════════════════════════════════════════════════════
#  JOURNAL CATEGORY ENUM
#  Exactly two members.  Each carries its label and log tag.
#  Enum drives all dispatch — no per-category if/elif chains needed.
# ══════════════════════════════════════════════════════════════════════════════

class _JournalCategory(Enum):
    """
    ENUM: _JournalCategory

    Each member value is a tuple of  (label, log_tag).

        label   – used in the merged output filename prefix
        log_tag – appears in every logger call for that category
    """
    #                    label                log_tag
    CUSTOMER = ('customer_journal', '[CUSTOMER]')
    UI       = ('ui_journal',       '[UI]')
    LLM      = ('journal_llm',       '[LLM]') 

    def __init__(self, label: str, log_tag: str) -> None:
        self.label   = label
        self.log_tag = log_tag


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS 
# ══════════════════════════════════════════════════════════════════════════════

def _read_text(p: Path) -> str:
    """
    FUNCTION: _read_text

    DESCRIPTION:
        Read a file trying common encodings, falling back to latin1.
        Exact copy of the helper already in routes.py — kept here so
        this module is fully self-contained.

    PARAMETERS:
        p (Path) : File path to read.

    RETURNS:
        str : File text content.
    """
    for enc in ('utf-8', 'latin1', 'windows-1252', 'utf-16'):
        try:
            return p.read_text(encoding=enc, errors='strict')
        except Exception:
            continue
    return p.read_text(encoding='latin1', errors='replace')


def _extract_date(filename: str) -> Optional[datetime]:
    """
    FUNCTION: _extract_date

    DESCRIPTION:
        Parse the date from a .jrn filename stem.
        Mirrors the extract_date() function you wrote in routes.py.
        Expects the stem to be exactly YYYYMMDD (e.g. '20250311').

    PARAMETERS:
        filename (str) : Bare filename, e.g. '20250311.jrn'

    RETURNS:
        datetime | None
    """
    name = Path(filename).stem
    try:
        return datetime.strptime(name, "%Y%m%d")
    except ValueError:
        return None


def _build_filename(label: str, start: datetime, end: datetime) -> str:
    """
    FUNCTION: _build_filename

    DESCRIPTION:
        Build the merged output filename from label + date range.
        Matches the format from your existing code:
            customer_journal_11march25_to_04april25.jrn

    PARAMETERS:
        label (str)      : Category label e.g. 'customer_journal'.
        start (datetime) : Earliest file date.
        end   (datetime) : Latest file date.

    RETURNS:
        str : e.g. 'customer_journal_11march25_to_04april25.jrn'
    """
    start_str = (start.strftime('%d%B%y').lstrip('0')).lower()   # '11march25'
    end_str   = (end.strftime('%d%B%y').lstrip('0')).lower()     # '04april25'

    if start_str == end_str:
        return f"{label}_{start_str}.jrn"

    return f"{label}_{start_str}_to_{end_str}.jrn"


# ══════════════════════════════════════════════════════════════════════════════
#  CORE MERGE  (one category at a time)
# ══════════════════════════════════════════════════════════════════════════════

def _merge_one(
    category : _JournalCategory,
    paths    : List[str],
) -> MergeResult:
    """
    FUNCTION: _merge_one

    DESCRIPTION:
        Merges all .jrn files in 'paths' for a single category into one
        StringIO buffer.  Logic is identical to what you wrote in routes.py:

          1. Collect files that have a parseable YYYYMMDD date in their name.
          2. Log a warning for any file whose name doesn't match.
          3. Sort by date ascending.
          4. Build the output filename from start_date → end_date.
          5. Write each file into the buffer wrapped with START / END markers.

        Nothing is written to disk or stored in the session here.
        The caller (FlatFileMerger.run) decides what to do with the result.

    PARAMETERS:
        category (_JournalCategory) : CUSTOMER or UI enum member.
        paths    (List[str])        : Full disk paths from file_categories dict.

    RETURNS:
        MergeResult : filename, merged content string, and count of merged files.
    """
    tag = category.log_tag

    # ── 1. Collect files with valid dates ────────────────────────────────────
    dated_files: List[Tuple[datetime, Path]] = []

    for path_str in paths:
        p = Path(path_str)
        file_date = _extract_date(p.name)
        if file_date:
            dated_files.append((file_date, p))
        else:
            logger.warning(f"{tag} Invalid date format in filename: {p.name}")

    if not dated_files:
        logger.info(f"{tag} No dated .jrn files found — nothing to merge.")
        return MergeResult(filename=None, content=None, file_count=0)

    # ── 2. Sort by date ascending ─────────────────────────────────────────────
    dated_files.sort(key=lambda x: x[0])

    # ── 3. Build output filename ──────────────────────────────────────────────
    start_date = dated_files[0][0]
    end_date   = dated_files[-1][0]
    out_filename = _build_filename(category.label, start_date, end_date)

    # ── 4. Merge into StringIO buffer ─────────────────────────────────────────
    combined_buffer = StringIO()

    for _, p in dated_files:
        file_name = p.name
        try:
            content = _read_text(p).strip()

            combined_buffer.write(
                f"\n========== START OF FILE: {file_name} ==========\n"
            )
            combined_buffer.write(content)
            combined_buffer.write(
                f"\n========== END OF FILE: {file_name} ==========\n"
            )

            logger.debug(f"{tag} Merged: {file_name}")

        except Exception as exc:
            logger.error(f"{tag} Failed during merge: {file_name} -> {exc}")

    combined_buffer.seek(0)
    merged_content = combined_buffer.getvalue()

    # ── 5. Summary logs ───────────────────────────────────────────────────────
    logger.info(f"{tag} Merged file name  : {out_filename}")
    logger.info(f"{tag} Total files merged: {len(dated_files)}")
    logger.info(f"{tag} Merged content size: {len(merged_content)} characters")

    return MergeResult(
        filename   = out_filename,
        content    = merged_content,
        file_count = len(dated_files),
    )


# ══════════════════════════════════════════════════════════════════════════════
#  OPTIONAL DISK WRITE  (for verification / debugging)
# ══════════════════════════════════════════════════════════════════════════════

def _write_to_disk(result: MergeResult, output_dir: Path, tag: str) -> None:
    """
    FUNCTION: _write_to_disk

    DESCRIPTION:
        Writes a MergeResult to disk so you can physically open and verify
        the merged file.  Only called when write_to_disk=True is passed to
        FlatFileMerger.run().

        Mirrors the commented-out block you already had in routes.py.

    PARAMETERS:
        result     (MergeResult) : The merge result to write.
        output_dir (Path)        : Directory to write into (created if absent).
        tag        (str)         : Log tag for this category.
    """
    if not result.filename or not result.content:
        return

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        out_path = output_dir / result.filename
        out_path.write_text(result.content, encoding='utf-8')
        logger.info(f"{tag} File written to disk: {out_path}")
    except Exception as exc:
        logger.error(f"{tag} Failed to write file to disk: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC CLASS  —  the only symbol routes.py imports
# ══════════════════════════════════════════════════════════════════════════════

class FlatFileMerger:
    """
    CLASS: FlatFileMerger

    DESCRIPTION:
        Public interface for routes.py.
        Reads .jrn files directly from disk paths, merges them into a
        StringIO buffer per category, and returns the result.

        
        Optionally writes to disk for physical verification.

    USAGE IN routes.py (two lines only):
    ──────────────────────────────────────
        from modules.flat_file_merger import FlatFileMerger

        FlatFileMerger.run(
            customer_paths = file_categories.get('customer_journals', []),
            ui_paths       = file_categories.get('ui_journals', []),
        )

        # To also write merged files to disk for verification:
        FlatFileMerger.run(
            customer_paths = file_categories.get('customer_journals', []),
            ui_paths       = file_categories.get('ui_journals', []),
            write_to_disk  = True,
            output_dir     = Path("merged_output"),   # optional, defaults to ./merged_output
        )
    """

    @staticmethod
    def run(
        customer_paths : List[str],
        ui_paths       : List[str],
        llm_paths      : List[str],
        write_to_disk  : bool          = False,
        output_dir     : Optional[Path] = None,
    ) -> dict:
        """
        FUNCTION: run

        DESCRIPTION:
            Entry point called from routes.py.
            Merges Customer Journal and UI Journal .jrn files independently.
            The _JournalCategory enum drives iteration — no per-category
            for-loop or if/elif chain exists here.

        PARAMETERS:
            customer_paths (List[str])   : Full disk paths from
                                           file_categories['customer_journals'].
            ui_paths       (List[str])   : Full disk paths from
                                           file_categories['ui_journals'].
            write_to_disk  (bool)        : If True, also write merged files to
                                           disk so you can physically verify them.
                                           Default: False.
            output_dir     (Path|None)   : Where to write files when
                                           write_to_disk=True.
                                           Default: Path('./merged_output').

        RETURNS:
            dict  —  { category_label: MergeResult }

            Example:
            {
              'customer_journal': MergeResult(
                  filename   = 'customer_journal_11march25_to_04april25.jrn',
                  content    = '... full merged text ...',
                  file_count = 5,
              ),
              'ui_journal': MergeResult(
                  filename   = 'ui_journal_01april25.jrn',
                  content    = '... full merged text ...',
                  file_count = 3,
              ),
            }
        """
        logger.info("[FlatFileMerger] Starting .jrn merge for Customer and UI journals.")

        # Map each enum member to the disk paths it should process
        paths_map = {
            _JournalCategory.CUSTOMER : customer_paths,
            _JournalCategory.UI       : ui_paths,
            _JournalCategory.LLM      : llm_paths,
        }

        # Resolve output directory once (used only when write_to_disk=True)
        disk_dir = output_dir or Path('merged_output')

        # Merge every category — enum drives the iteration via map()
        def _process(cat: _JournalCategory):
            result = _merge_one(cat, paths_map[cat])
            if write_to_disk:
                _write_to_disk(result, disk_dir, cat.log_tag)
            return cat.label, result

        results = dict(map(_process, _JournalCategory))

        merged_count = sum(1 for r in results.values() if r.content)
        logger.info(
            f"[FlatFileMerger] Done — "
            f"{merged_count}/{len(_JournalCategory)} categories merged."
        )
        return results