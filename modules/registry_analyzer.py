import re
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
from modules.logging_config import logger
import logging


logger.info("Starting app_logger.file_content_detector")


SECTION_RE = re.compile(r"^\s*\[(.+?)\]\s*$")
KV_RE = re.compile(r'^\s*(@|".+?"|[^=]+?)\s*=\s*(.+?)\s*$')

class RegistryAnalyzerService:
    """
    FUNCTION:
        RegistryAnalyzerService

    DESCRIPTION:
        Service class responsible for parsing, decoding, analyzing,
        and comparing Windows Registry (.reg) files. It replicates
        the logic of standard registry parsing tools and exposes
        helper functions for data extraction and comparison.

    USAGE:
        service = RegistryAnalyzerService()
        diff = service.compare_registry_files("old.reg", "new.reg")

    PARAMETERS:
        None

    RETURNS:
        None

    RAISES:
        None
    """

    def _safe_decode_lines(self, blob: bytes) -> List[str]:
        """
        FUNCTION:
            _safe_decode_lines

        DESCRIPTION:
            Safely decodes raw registry file bytes using multiple encodings
            until decoding succeeds. Falls back to UTF-8 with replacement
            if all other decoders fail.

        USAGE:
            lines = self._safe_decode_lines(blob)

        PARAMETERS:
            blob (bytes) :
                Raw file content in bytes format.

        RETURNS:
            list[str] :
                List of decoded text lines.

        RAISES:
            None
        """
        logger.debug("Starting safe decode for registry file content.")
        encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
        for e in encs:
            try:
                lines = blob.decode(e).splitlines()
                logger.debug(f"Decoded blob successfully using encoding: {e}")
                return lines
            except Exception:
                logger.debug(f"Failed decoding with encoding: {e}")
                continue
        logger.error("All decoders failed. Falling back to utf-8 with replacement.")
        return blob.decode("utf-8", errors="replace").splitlines()

    def _normalize_key(self, raw: str) -> str:
        """
        FUNCTION:
            _normalize_key

        DESCRIPTION:
            Normalizes registry key formatting by stripping whitespace,
            removing quotes, and handling "@" default key cases.

        USAGE:
            normalized = self._normalize_key(key_string)

        PARAMETERS:
            raw (str) :
                Raw key string extracted from registry file.

        RETURNS:
            str :
                Cleaned and normalized registry key.

        RAISES:
            None
        """
        logger.debug(f"Normalizing registry key: {raw}")
        s = (raw or "").strip()
        if s == "@":
            return s
        if s.startswith('"') and s.endswith('"'):
            return s[1:-1]
        return s

    def _parse_lines(self, lines: List[str]) -> List[Dict[str, str]]:
        """
        FUNCTION:
            _parse_lines

        DESCRIPTION:
            Parses registry lines into structured dictionaries containing
            section names (registry paths), keys, and values.

        USAGE:
            rows = self._parse_lines(lines)

        PARAMETERS:
            lines (list[str]) :
                List of decoded registry file lines.

        RETURNS:
            list[dict] :
                Each dict includes:
                    - Device Path
                    - Key
                    - Value

        RAISES:
            None
        """
        logger.debug("Parsing lines from registry file.")
        rows: List[Dict[str, str]] = []
        current_section: str | None = None
        seen_kv = False
        i, n = 0, len(lines)
        while i < n:
            raw = lines[i].rstrip("\n")
            line = raw.strip()
            i += 1
            if not line:
                continue
            m = SECTION_RE.match(line)
            if m:
                logger.debug(f"Found section: {m.group(1)}")
                if current_section and not seen_kv:
                    rows.append({"Device Path": current_section, "Key": "", "Value": ""})
                current_section = m.group(1).strip()
                seen_kv = False
                continue
            if current_section:
                mv = KV_RE.match(line)
                if mv:
                    logger.debug(f"Found key-value under section {current_section}: {line}")
                    kraw, vraw = mv.groups()
                    vfull = vraw
                    while vfull.endswith("\\") and i < n:
                        cont = lines[i].rstrip("\n")
                        i += 1
                        vfull = vfull[:-1] + cont.strip()
                    rows.append({
                        "Device Path": current_section,
                        "Key": self._normalize_key(kraw),
                        "Value": vfull.strip()
                    })
                    seen_kv = True
                    continue
        if current_section and not seen_kv:
            rows.append({"Device Path": current_section, "Key": "", "Value": ""})
        logger.debug("Completed parsing registry lines.")
        return rows

    def _parse_reg_file_to_df(self, file_path: str) -> pd.DataFrame:
        """
        FUNCTION: _parse_reg_file_to_df

        DESCRIPTION:
        Reads a registry file, decodes its content, parses registry entries,
        and converts them into a pandas DataFrame.

        USAGE:
        df = self._parse_reg_file_to_df("file1.reg")

        PARAMETERS:
        file_path (str) : Path to registry (.reg) file.

        RETURNS:
        pandas.DataFrame :
        Columns: ["Device Path", "Key", "Value"]

        RAISES:
        FileNotFoundError : If the file does not exist.
        """
        logger.info(f"Parsing registry file: {file_path}")

        if not Path(file_path).exists():
            logger.error(f"Registry file not found: {file_path}")
            raise FileNotFoundError(f"Registry file not found: {file_path}")

        with open(file_path, 'rb') as f:
            blob = f.read()
        
        lines = self._safe_decode_lines(blob)
        rows = self._parse_lines(lines)
        
        if not rows:
            logger.warning(f"No valid registry entries found in: {file_path}")
            return pd.DataFrame(columns=["Device Path", "Key", "Value"])

        df = pd.DataFrame(rows)
        logger.info(f"Parsed {len(df)} registry entries from {file_path}")
        return df

    def view_registry_file(self, file_path: str) -> Dict[str, Any]:
        """
        FUNCTION:
            view_registry_file

        DESCRIPTION:
            Parses a registry file and returns results in JSON-friendly
            dictionary form. Includes parsed entries and count. If parsing
            fails, returns raw file content and the error message.

        USAGE:
            response = self.view_registry_file("settings.reg")

        PARAMETERS:
            file_path (str) :
                Path to the registry file being viewed.

        RETURNS:
            dict :
                {
                    "parsed": bool,
                    "entries": list_of_dicts OR None,
                    "count": int,
                    "raw_content": str (if error),
                    "error": str (if error)
                }

        RAISES:
            None
        """
        logger.info(f"Viewing registry file: {file_path}")
        try:
            df = self._parse_reg_file_to_df(file_path)
            logger.info(f"Successfully parsed registry file: {file_path}")
            return {
                "parsed": True,
                "entries": df.to_dict('records'),
                "count": len(df)
            }
        except Exception as e:
            logger.error(f"Failed to parse registry file {file_path}: {e}")
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                raw_content = f.read()
            return {
                "parsed": False,
                "raw_content": raw_content,
                "error": str(e)
            }

    def compare_registry_files(self, file1_path: str, file2_path: str) -> Dict[str, Any]:

        """
        FUNCTION: compare_registry_files

        DESCRIPTION:
        Compares two registry files by reading and parsing both, merging
        their content, and identifying added, removed, changed, and identical
        entries.

        USAGE:
        result = service.compare_registry_files("a.reg", "b.reg")

        PARAMETERS:
        file1_path (str) : Path to first registry file.
        file2_path (str) : Path to second registry file.

        RETURNS:
        dict :
            {
            "changed": list,
            "added": list,
            "removed": list,
            "identical_count": int
            }

        RAISES:
        FileNotFoundError : When either file does not exist.
        """
        logger.info(f"Comparing registry files: {file1_path} <-> {file2_path}")

        df_a = self._parse_reg_file_to_df(file1_path)
        df_b = self._parse_reg_file_to_df(file2_path)

        if df_a.empty and df_b.empty:
            logger.info("Both registry files are empty. Nothing to compare.")
            return {
                "changed": [],
                "added": [],
                "removed": [],
                "identical_count": 0
            }

        # Merge dataframes to find differences
        merged = df_a.merge(
            df_b, 
            on=["Device Path", "Key"], 
            how="outer", 
            suffixes=("_A", "_B"), 
            indicator=True
        )

        # Entries only in File A (removed)
        removed_df = merged[merged["_merge"] == "left_only"]
        removed_list = removed_df[["Device Path", "Key", "Value_A"]].rename(columns={"Value_A": "Value"}).to_dict('records')

        # Entries only in File B (added)
        added_df = merged[merged["_merge"] == "right_only"]
        added_list = added_df[["Device Path", "Key", "Value_B"]].rename(columns={"Value_B": "Value"}).to_dict('records')

        # Entries in both files
        both_df = merged[merged["_merge"] == "both"].copy()
        
        # Fill NaN to handle cases where a value is present in one but not the other
        both_df['Value_A'] = both_df['Value_A'].fillna('')
        both_df['Value_B'] = both_df['Value_B'].fillna('')

        # Find changed values
        changed_df = both_df[both_df["Value_A"] != both_df["Value_B"]]
        changed_list = changed_df[["Device Path", "Key", "Value_A", "Value_B"]].to_dict('records')

        # Find identical entries
        identical_count = len(both_df[both_df["Value_A"] == both_df["Value_B"]])

        logger.info(f"Comparison completed: changed={len(changed_list)}, added={len(added_list)}, removed={len(removed_list)}, identical={identical_count}")

        return {
            "changed": changed_list,
            "added": added_list,
            "removed": removed_list,
            "identical_count": identical_count
        }