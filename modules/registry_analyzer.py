import re
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
from modules.logging_config import logger


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
            Safely decodes raw registry file bytes using BOM/magic-byte
            sniffing first, then falls back to sequential encoding attempts.
            Avoids redundant full-blob decodes by detecting encoding upfront.

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
        # FIX 1: Sniff BOM / magic bytes first to avoid trying all encodings
        # on every file. The vast majority of .reg files are UTF-16 LE with BOM
        # or UTF-8 with BOM — we can detect that in O(1) from the first bytes.
        if blob[:2] in (b'\xff\xfe', b'\xfe\xff'):
            enc = "utf-16"
        elif blob[:3] == b'\xef\xbb\xbf':
            enc = "utf-8-sig"
        else:
            enc = None

        if enc:
            try:
                return blob.decode(enc).splitlines()
            except Exception:
                pass  # Fall through to sequential attempt

        # Only reach here for ambiguous encodings
        for e in ("cp1252", "latin-1", "utf-8"):
            try:
                return blob.decode(e).splitlines()
            except Exception:
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

            Optimized: avoids per-line regex where possible by using
            fast character checks before falling back to compiled patterns.
            Also avoids repeated string allocations with strip() in the hot path.

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
        rows: List[Dict[str, str]] = []
        current_section: str | None = None
        seen_kv = False
        i, n = 0, len(lines)

        # FIX 2: Cache bound methods and avoid repeated attribute lookups
        # inside the hot loop.
        section_match = SECTION_RE.match
        kv_match = KV_RE.match
        normalize = self._normalize_key
        rows_append = rows.append

        while i < n:
            line = lines[i].strip()
            i += 1

            # FIX 3: Fast-path empty line and comment skip before any regex.
            if not line or line.startswith(';'):
                continue

            # FIX 4: Use first-char check to decide which regex to try,
            # avoiding running both patterns on every line.
            if line[0] == '[':
                m = section_match(line)
                if m:
                    if current_section and not seen_kv:
                        rows_append({"Device Path": current_section, "Key": "", "Value": ""})
                    current_section = m.group(1).strip()
                    seen_kv = False
                    continue

            if current_section and '=' in line:
                mv = kv_match(line)
                if mv:
                    kraw, vraw = mv.groups()
                    # FIX 5: Build continuation string once via list join
                    # rather than repeated string concatenation (O(n) vs O(n²)).
                    if vraw.endswith("\\"):
                        parts = [vraw[:-1]]
                        while i < n and vraw.endswith("\\"):
                            vraw = lines[i].strip()
                            i += 1
                            if vraw.endswith("\\"):
                                parts.append(vraw[:-1])
                            else:
                                parts.append(vraw)
                        vfull = "".join(parts)
                    else:
                        vfull = vraw

                    rows_append({
                        "Device Path": current_section,
                        "Key": normalize(kraw),
                        "Value": vfull.strip()
                    })
                    seen_kv = True

        if current_section and not seen_kv:
            rows_append({"Device Path": current_section, "Key": "", "Value": ""})

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
        Compares two registry files by reading and parsing both, then
        performing a dict-based diff instead of a Pandas merge. This avoids
        the overhead of outer-merge + indicator column allocation and is
        significantly faster for typical registry file sizes.

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
            return {"changed": [], "added": [], "removed": [], "identical_count": 0}

        # FIX 6: Replace pandas outer-merge+indicator with dict lookups.
        # Building dicts is O(n); lookups are O(1). The merge approach
        # allocates a combined DataFrame and iterates it multiple times.
        def to_dict(df: pd.DataFrame) -> dict:
            result = {}
            for row in df.itertuples(index=False):
                result[(row[0], row[1])] = row[2]  # (Device Path, Key) -> Value
            return result

        map_a = to_dict(df_a)
        map_b = to_dict(df_b)

        changed, added, removed = [], [], []
        identical_count = 0

        keys_a = set(map_a)
        keys_b = set(map_b)

        for key in keys_a & keys_b:
            val_a, val_b = map_a[key], map_b[key]
            if val_a == val_b:
                identical_count += 1
            else:
                changed.append({
                    "Device Path": key[0],
                    "Key": key[1],
                    "Value_A": val_a,
                    "Value_B": val_b,
                })

        for key in keys_a - keys_b:
            removed.append({"Device Path": key[0], "Key": key[1], "Value": map_a[key]})

        for key in keys_b - keys_a:
            added.append({"Device Path": key[0], "Key": key[1], "Value": map_b[key]})

        logger.info(
            f"Comparison completed: changed={len(changed)}, added={len(added)}, "
            f"removed={len(removed)}, identical={identical_count}"
        )
        return {
            "changed": changed,
            "added": added,
            "removed": removed,
            "identical_count": identical_count,
        }