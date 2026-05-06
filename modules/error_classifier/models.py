from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    P1 = "P1"   # Critical / Exception
    P2 = "P2"   # Error / Reboot
    P3 = "P3"   # Warning
    P5 = "P5"   # Verbose / Info / Trace

    @property
    def label(self) -> str:
        return {
            "P1": "Critical/Exception",
            "P2": "Error/Reboot",
            "P3": "Warning",
            "P5": "Verbose",
        }[self.value]

    @property
    def sort_order(self) -> int:
        return {"P1": 0, "P2": 1, "P3": 2, "P5": 3}[self.value]


@dataclass
class ErrorRecord:
    trace: str          # StCode value or TRCTRACE trace-type column
    severity: Severity
    source: str         # Module / DLL name
    message: str        # Add: content or TRCTRACE body text
    type_error: str     # Full TypeError block if present (else empty string)
    source_file: str    # "TRCERROR" or "TRCTRACE"

    _dedup_key: str = field(default="", repr=False)

    def __post_init__(self):
        if not self._dedup_key:
            self._dedup_key = f"{self.trace}||{self.severity}||{self.source}||{self.message}"