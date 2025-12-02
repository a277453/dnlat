import re
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd

SECTION_RE = re.compile(r"^\s*\[(.+?)\]\s*$")
KV_RE = re.compile(r'^\s*(@|".+?"|[^=]+?)\s*=\s*(.+?)\s*$')

class RegistryAnalyzerService:
    """
    Service for parsing and comparing Windows Registry (.reg) files.
    Logic replicated from the reference registry tool.
    """

    def _safe_decode_lines(self, blob: bytes) -> List[str]:
        encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
        for e in encs:
            try:
                return blob.decode(e).splitlines()
            except Exception:
                continue
        return blob.decode("utf-8", errors="replace").splitlines()

    def _normalize_key(self, raw: str) -> str:
        s = (raw or "").strip()
        if s == "@":
            return s
        if s.startswith('"') and s.endswith('"'):
            return s[1:-1]
        return s

    def _parse_lines(self, lines: List[str]) -> List[Dict[str, str]]:
        """Core parsing logic for .reg file content."""
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
                if current_section and not seen_kv:
                    rows.append({"Device Path": current_section, "Key": "", "Value": ""})
                current_section = m.group(1).strip()
                seen_kv = False
                continue
            if current_section:
                mv = KV_RE.match(line)
                if mv:
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
        return rows

    def _parse_reg_file_to_df(self, file_path: str) -> pd.DataFrame:
        """Parses a .reg file into a pandas DataFrame."""
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Registry file not found: {file_path}")

        with open(file_path, 'rb') as f:
            blob = f.read()
        
        lines = self._safe_decode_lines(blob)
        rows = self._parse_lines(lines)
        
        if not rows:
            return pd.DataFrame(columns=["Device Path", "Key", "Value"])

        df = pd.DataFrame(rows)
        return df

    def view_registry_file(self, file_path: str) -> Dict[str, Any]:
        """
        Returns the content of a single registry file as a structured list of records.
        """
        try:
            df = self._parse_reg_file_to_df(file_path)
            return {
                "parsed": True,
                "entries": df.to_dict('records'),
                "count": len(df)
            }
        except Exception as e:
            # Fallback to raw text if parsing fails
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                raw_content = f.read()
            return {
                "parsed": False,
                "raw_content": raw_content,
                "error": str(e)
            }

    def compare_registry_files(self, file1_path: str, file2_path: str) -> Dict[str, Any]:
        """
        Performs a structured comparison of two registry files.
        """
        df_a = self._parse_reg_file_to_df(file1_path)
        df_b = self._parse_reg_file_to_df(file2_path)

        if df_a.empty and df_b.empty:
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

        return {
            "changed": changed_list,
            "added": added_list,
            "removed": removed_list,
            "identical_count": identical_count
        }