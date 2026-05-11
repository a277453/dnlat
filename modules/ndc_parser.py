"""
ndc_parser.py
=============
Decodes raw CCProtFW1 NDC protocol messages from DNLAT journal logs
into human-readable structured form.

Based on analysis of real DN ATM logs (20250915.jrn).

Real message classes present in logs:
    11  Transaction Request – Card Data          ATM->HOST  (52×)
    22  Go Next State / Acknowledge              HOST->ATM  (539×)
    40  Transaction Complete                     ATM->HOST  (12×)
    61  Solicited Status                         ATM->HOST  (6×)
    10  Go Out of Service                        HOST->ATM  (48×)
    12  Status Ready (ATM->HOST init/config)      ATM->HOST  (19×)
    30  Go In Service / Screen Load              HOST->ATM  (358×)
    80  EMV / Encryption Config Download         HOST->ATM  (96×)
    23  Cancel                                   HOST->ATM  (5×)

Public API:
    decode_message(raw: str) -> NdcMessage
    decode_log_block(raw_block: str) -> list[NdcMessage]
    extract_messages_from_jrn(jrn_text: str) -> list[NdcMessage]
    parse_ndc_log_file(file_path) -> list[NdcMessage]
"""

import re
from dataclasses import dataclass, field
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Wire protocol constants
# ─────────────────────────────────────────────────────────────────────────────

FS = "\x1c"
GS = "\x1d"

# ─────────────────────────────────────────────────────────────────────────────
# Message registry
# ─────────────────────────────────────────────────────────────────────────────

MESSAGE_CLASS_MAP = {
    "11": ("Transaction Request – Card Data",          "ATM->HOST"),
    "12": ("Status / Ready",                           "ATM->HOST"),
    "23": ("Encryptor Initialisation Data",            "ATM->HOST"),
    "61": ("Solicited Status",                         "ATM->HOST"),
    "62": ("Solicited Status – Command Reject",        "ATM->HOST"),
    "70": ("Unsolicited Status – Device Fault",        "ATM->HOST"),
    "71": ("Unsolicited Status – Ready",               "ATM->HOST"),
    "40": ("Transaction Reply Command",                "HOST->ATM"),
    "41": ("Transaction Reply – No Receipt",           "HOST->ATM"),
    "6":  ("EJ Command",                              "HOST->ATM"),
    "10": ("Go Out of Service",                        "HOST->ATM"),
    "22": ("Go Next State / Acknowledge",              "HOST->ATM"),
    "24": ("Send PIN / Cancel",                        "HOST->ATM"),
    "30": ("Go In Service / Screen Load",              "HOST->ATM"),
    "31": ("FIT Data Load",                            "HOST->ATM"),
    "80": ("EMV / Encryption Config Download",         "HOST->ATM"),
    "83": ("Supervisor Reply",                         "HOST->ATM"),
    "84": ("Go Out of Service (Supervisor)",           "HOST->ATM"),
    "85": ("Go In Service (Supervisor)",               "HOST->ATM"),
}

# ─────────────────────────────────────────────────────────────────────────────
# Lookup tables
# ─────────────────────────────────────────────────────────────────────────────

GOS_REASON_CODES = {
    "1":  "Transaction successful – Go In Service follows",
    "2":  "Command reject (e.g. supervisor active)",
    "8":  "Specific reject (device fault)",
    "71": "Cassette configuration download",
    "72": "Screen/state table download",
    "73": "Encryption key download",
    "75": "EMV AID / CA key download",
    "1E": "Extended download",
}

GIS_FUNCTION_CODES = {
    "11": "State/screen table download",
    "12": "Screen data download (continued)",
    "15": "Cassette / denomination config",
    "16": "Receipt format",
    "1A": "Sensor config",
    "1C": "Screen table (extended)",
    "1E": "Extended state table",
    "42": "Fitness data / counter reset",
}

STATUS_READY_SUBTYPES = {
    "B": "Configuration Complete",
    "E": "Cassette Config Complete",
    "P": "Peripheral Status",
}

NEXT_STATE_CODES = {
    "9": "Supervisor / Idle (ACK)",
    "B": "Transaction state",
    "C": "State C",
    "F": "Configuration sequence",
}

EMV_TAG_MAP = {
    "5F2A": ("Transaction Currency Code",        "n"),
    "5F34": ("Application PAN Sequence No.",     "n"),
    "5F36": ("Transaction Currency Exponent",    "n"),
    "82":   ("Application Interchange Profile",  "b"),
    "84":   ("Dedicated File Name",              "b"),
    "8A":   ("Authorisation Response Code",      "an"),
    "91":   ("Issuer Auth Data",                 "b"),
    "95":   ("Terminal Verification Results",    "b"),
    "9A":   ("Transaction Date",                 "n"),
    "9B":   ("Transaction Status Info",          "b"),
    "9C":   ("Transaction Type",                 "n"),
    "9F02": ("Amount Authorised",                "n"),
    "9F03": ("Amount Other",                     "n"),
    "9F06": ("Application Identifier",           "b"),
    "9F09": ("App Version Number",               "b"),
    "9F10": ("Issuer Application Data",          "b"),
    "9F12": ("App Preferred Name",               "ans"),
    "9F1A": ("Terminal Country Code",            "n"),
    "9F1E": ("IFD Serial Number",                "an"),
    "9F26": ("Application Cryptogram",           "b"),
    "9F27": ("Cryptogram Information Data",      "b"),
    "9F33": ("Terminal Capabilities",            "b"),
    "9F34": ("CVM Results",                      "b"),
    "9F35": ("Terminal Type",                    "n"),
    "9F36": ("Application Transaction Counter",  "n"),
    "9F37": ("Unpredictable Number",             "b"),
    "9F41": ("Transaction Sequence Counter",     "n"),
    "57":   ("Track 2 Equivalent Data",          "b"),
    "77":   ("Response Message Template Fmt2",   "b"),
}

CRYPTOGRAM_TYPE = {
    "00": "AAC (Transaction Declined)",
    "40": "TC  (Transaction Approved)",
    "80": "ARQC (Online Auth Request)",
    "C0": "RFU",
}

TRANSACTION_TYPES = {
    "00": "Purchase",
    "01": "Cash Withdrawal",
    "09": "Purchase with Cashback",
    "20": "Balance Enquiry",
    "21": "Mini Statement",
    "30": "PIN Change",
    "31": "PIN Unblock",
    "34": "PIN Change (ICC)",
    "40": "Deposit",
    "84": "Fast Cash",
    "90": "Balance Enquiry (ICC)",
    "91": "Mini Statement (ICC)",
    "92": "Fund Transfer",
    "93": "Bill Payment",
}

# Operation Code (field j) — 8 positions, each maps to an FDK key
# 'A' = pressed, ' ' = not pressed, other chars = data-entry state identifiers
FDK_POSITIONS = ["FDK-A", "FDK-B", "FDK-C", "FDK-D", "FDK-F", "FDK-G", "FDK-H", "FDK-I"]

# Last Status Issued codes (inside field r, byte offset 4)
LAST_STATUS_CODES = {
    "0": "None sent",
    "1": "Good termination sent",
    "2": "Error status sent",
    "3": "Transaction reply rejected",
}

# Top-of-Receipt Transaction Flag (field f)
TOP_OF_RECEIPT_FLAGS = {
    "0": "Will NOT print at top of receipt",
    "1": "Will print at top of receipt",
}

# Encryptor Initialisation Data — Information Identifier codes (class 23, field[3])
ENCRYPTOR_INFO_IDS = {
    "1": "EPP Serial Number and Signature",
    "2": "EPP Public Key and Signature",
    "3": "New Key Verification Value (KVV)",
    "4": "Keys Status (Master/Comms/MAC/B Key KVVs)",
    "5": "Public Key Loaded",
    "6": "Key Entry Mode",
    "7": "Certificate RSA Encryption KVV",
    "8": "SST Certificate",
    "9": "SST Random Number",
    "A": "PKCS7 Key Loaded",
    "B": "Encryptor Capabilities and State",
    "C": "Key Deleted",
    "D": "EPP Attributes",
    "E": "Variable-length EPP Serial Number and Signature",
    "H": "Host Certificate",
    "I": "EPP Unbound",
    "J": "Extended Capabilities",
    "K": "Extended Key Status",
    "L": "Authentication Token",
}

KEY_ENTRY_MODES = {
    "1": "Single length without XOR",
    "2": "Single length with XOR",
    "3": "Double length with XOR",
    "4": "Double length, restricted",
}

# Supply status codes (class 12 subtype E, one char per cassette/bin)
SUPPLY_STATUS_CODES = {
    "0": "OK",
    "1": "Low",
    "2": "Empty",
    "3": "Inoperative",
    "4": "Manipulated",
    "5": "Replenishment Requested",
    "6": "Tampered",
    "7": "Fraud Suspected",
    "8": "Invalid",
}

SUPPLY_DEVICE_LABELS = [
    "Cassette 1", "Cassette 2", "Cassette 3", "Cassette 4",
    "Reject Bin", "Cassette 5", "Cassette 6", "Cassette 7", "Cassette 8",
]

# EJ Command types (class 6, field[3] first char)
EJ_COMMAND_TYPES = {
    "1": "Acknowledge EJ Upload Block",
    "2": "Acknowledge and Stop EJ",
    "3": "EJ Options and Timers",
}

EJ_OPTIONS = {
    "60": ("EJ Upload Block Size",    "001–350 chars, default 200"),
    "61": ("EJ Retry Threshold",      "000–999 attempts, default 000 (infinite)"),
}

EJ_TIMER_NAMES = {
    "60": "EJ Acknowledgement Timer (seconds, default 255, 000=infinite)",
}

# Transaction Reply Function Identifiers (class 40/41, field l)
TXN_REPLY_FUNCTIONS = {
    "1": "Deposit and Print",
    "2": "Dispense and Print",
    "3": "Display and Print",
    "4": "Print Immediate",
    "7": "Deposit and Print (extended)",
    "8": "Dispense and Print (extended)",
    "9": "Display and Print (extended)",
}

DEVICE_STATUS_MAP = {
    0: "Card Reader",   1: "Cash Handler",    2: "Receipt Printer",
    3: "Journal Printer", 4: "Encryptor",     5: "Camera",
    6: "Door",          7: "Fascia",          8: "Supervisor",
    9: "Statement Printer", 10: "Passbook Printer", 11: "Barcode Reader",
    12: "Cheque Processing", 13: "Coin Dispenser",
}

DEVICE_STATE_CODES = {
    "0": "OK",                  "1": "Needs Replenishment",
    "2": "Inoperative",         "3": "Needs Maintenance",
    "4": "In Supervisor Mode",  "5": "Busy",
    "6": "Offline",             "7": "Error",
    "8": "Threshold",           "9": "Needs Maintenance",
    "A": "Serious Fault",       "B": "Hardware Error",
    "C": "Configuration Error", "D": "Disabled",
}

# ─────────────────────────────────────────────────────────────────────────────
# Data class
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NdcMessage:
    raw:           str
    message_class: str
    direction:     str
    summary:       str
    fields:        list = field(default_factory=list)
    emv_tags:      dict = field(default_factory=dict)
    receipt_lines: list = field(default_factory=list)
    errors:        list = field(default_factory=list)
    timestamp:     str  = ""   # HH:MM:SS from the 41004/41005 log line


# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def _unescape(raw: str) -> str:
    return re.sub(r"\\([0-9a-fA-F]{2})",
                  lambda m: bytes.fromhex(m.group(1)).decode("latin-1"), raw)

def _split_fs(msg: str) -> list:
    return msg.split(FS)

def _clean_text(raw: str) -> list:
    text = raw
    text = re.sub(r"\\x0[aA]", "\n", text); text = re.sub(r"\\0[aA]", "\n", text)
    text = re.sub(r"\\x0[cCdD]", "\n", text); text = re.sub(r"\\0[cCdD]", "\n", text)
    text = re.sub(r"\x1b\[[^a-zA-Z]*[a-zA-Z]", "", text)
    text = re.sub(r"\x1b[()P\\]?", "", text)
    text = text.replace(GS, "\n").replace(FS, "\n")
    text = re.sub(r"\x0f[^\x0a\x0d\x0c]*", " ", text)
    text = re.sub(r"[\x00-\x08\x0b\x0e\x1a\x1b]", "", text)
    return [l.strip() for l in re.split(r"[\n\r\f]", text)
            if l.strip() and not re.match(r"^[\x00-\x1f\s]+$", l.strip())]


# ─────────────────────────────────────────────────────────────────────────────
# EMV TLV decoder
# ─────────────────────────────────────────────────────────────────────────────

def _decode_emv_tlv(hex_str: str) -> dict:
    tags = {}
    data = hex_str.upper().strip()
    i = 0
    while i < len(data) - 3:
        tag = data[i:i+2]; i += 2
        if tag in ("1F", "9F", "5F", "DF"):
            if i + 2 <= len(data):
                tag += data[i:i+2]; i += 2
        if i + 2 > len(data): break
        try:
            length = int(data[i:i+2], 16)
        except ValueError:
            break
        i += 2
        if i + length * 2 > len(data): break
        value_hex = data[i:i+length*2]; i += length * 2
        tag_info = EMV_TAG_MAP.get(tag)
        if not tag_info:
            tags[f"[{tag}]"] = value_hex; continue
        tag_name, fmt = tag_info
        if fmt == "n":
            if tag in ("9F02", "9F03") and len(value_hex) == 12:
                try: tags[tag_name] = f"{int(value_hex)/100:.2f}"; continue
                except ValueError: pass
            if tag == "9A" and len(value_hex) == 6:
                tags[tag_name] = f"20{value_hex[0:2]}-{value_hex[2:4]}-{value_hex[4:6]}"; continue
            if tag == "9C":
                tags[tag_name] = TRANSACTION_TYPES.get(value_hex.upper(), value_hex); continue
            if tag == "9F27":
                try:
                    k = f"{int(value_hex[0:2], 16) & 0xC0:02X}"
                    tags[tag_name] = CRYPTOGRAM_TYPE.get(k, f"0x{value_hex}")
                except ValueError:
                    tags[tag_name] = value_hex
                continue
            tags[tag_name] = value_hex.lstrip("0") or "0"
        elif fmt == "ans":
            try: tags[tag_name] = bytes.fromhex(value_hex).decode("ascii", errors="replace")
            except Exception: tags[tag_name] = value_hex
        else:
            tags[tag_name] = value_hex
    return tags

def _decode_cam_block(raw: str) -> dict:
    """Decode GS-delimited CAM block.

    Two formats seen in the wild:
        Format A:  CAM\x1d<tlv_hex>       — TLV appended directly after "CAM"
        Format B:  \x1dCAM\x1d<tlv_hex>  — "CAM" is its own GS segment,
                                            TLV is the segment immediately after
    """
    tags = {}
    parts = raw.split(GS)
    i = 0
    while i < len(parts):
        part = parts[i]
        if part.strip().upper() == "CAM":
            # Format B: TLV is in the next segment
            if i + 1 < len(parts):
                tlv = parts[i + 1].strip()
                if tlv:
                    tags.update(_decode_emv_tlv(tlv))
            i += 2
        elif part.upper().startswith("CAM"):
            # Format A: TLV is appended directly after "CAM"
            tlv = part[3:].strip()
            if tlv:
                tags.update(_decode_emv_tlv(tlv))
            i += 1
        else:
            i += 1
    return tags

def _decode_5cam_block(raw: str) -> dict:
    if not raw.upper().startswith("5CAM"): return {}
    return _decode_emv_tlv(raw[4:].strip())


# ─────────────────────────────────────────────────────────────────────────────
# Per-class decoders
# ─────────────────────────────────────────────────────────────────────────────

def _decode_11(f: list, msg: NdcMessage) -> None:
    """Transaction Request – Card Data  (ATM->HOST)

    Spec field map (NDC Transaction Request Message Format):
        [0]  b+c  Message class+subclass = "11"
        [1]  d    LUNO (3 or 9 chars)
        [2]       FS — reserved
        [3]       FS — reserved
        [4]  e    Time Variant Number (8 hex chars)
        [5]  h    Track 2 Data (up to 39 chars, start->end sentinel)
        [6]  i    Track 3 Data (up to 106 chars, optional)
        [7]  j    Operation Code (8 chars — FDK key states)
        [8]  k    Amount Entry (8 or 12 chars, right-justified, ÷100)
        [9]  l    PIN Buffer / Buffer A (up to 32 chars, encrypted)
        [10] m    Buffer B — General Purpose (up to 32 chars)
        [11] n    Buffer C — General Purpose (up to 32 chars)
        [12] o+p  Track 1 ID ('1') + Track 1 Data (optional, up to 78 chars)
        [13] q+r  Transaction Status ID ('2') + Last Transaction Status block
                  Pre-GS layout of field r:
                    [0:4]   Last Transaction Serial Number (4 bytes)
                    [4]     Last Status Issued (1 byte)
                    [5:25]  Last Transaction Notes Dispensed (20 bytes)
                    [25:45] Last Transaction Coins Dispensed (20 bytes)
                    [45:]   Last Cash Deposit Direction (optional)
                  After GS: CAM\x1d<EMV TLV>
        [14] 5CAM issuer auth block (from previous transaction)
    """
    luno    = f[1].strip() if len(f) > 1 else ""
    tvn     = f[4].strip() if len(f) > 4 else ""
    trk2    = f[5].strip() if len(f) > 5 else ""
    trk3    = f[6].strip() if len(f) > 6 else ""
    op_code = f[7]         if len(f) > 7 else ""
    amount  = f[8].strip() if len(f) > 8 else ""
    buf_a   = f[9].strip() if len(f) > 9 else ""
    buf_b   = f[10].strip() if len(f) > 10 else ""
    buf_c   = f[11].strip() if len(f) > 11 else ""

    if luno: msg.fields.append(("LUNO",                luno))
    if tvn:  msg.fields.append(("Time Variant Number", tvn))

    # Track 2
    if trk2: msg.fields.append(("Track 2 (masked)",   trk2))
    if trk3: msg.fields.append(("Track 3",            trk3))

    # Operation Code — decode each FDK position
    if op_code:
        msg.fields.append(("─── Operation Code ───", ""))
        pressed = [FDK_POSITIONS[i] for i, c in enumerate(op_code[:8])
                   if c == 'A']
        data_chars = [(FDK_POSITIONS[i], c) for i, c in enumerate(op_code[:8])
                      if c not in ('A', ' ') and i < len(FDK_POSITIONS)]
        msg.fields.append(("  Raw",         op_code.rstrip()))
        if pressed:
            msg.fields.append(("  Keys Pressed", ", ".join(pressed)))
        for pos, char in data_chars:
            msg.fields.append((f"  {pos} State",  char))

    # Amount
    if amount and amount.strip("0"):
        try:
            msg.fields.append(("Amount", f"{int(amount) / 100:.2f}"))
        except ValueError:
            msg.fields.append(("Amount (raw)", amount))

    # Buffers
    if buf_a: msg.fields.append(("PIN Buffer (A)", buf_a))
    if buf_b: msg.fields.append(("Buffer B",       buf_b))
    if buf_c: msg.fields.append(("Buffer C",       buf_c))

    # Track 1 (optional — field [12] starts with '1' identifier)
    if len(f) > 12:
        trk1_field = f[12].strip()
        if trk1_field.startswith("1"):
            msg.fields.append(("Track 1 Data", trk1_field[1:]))

    # Last Transaction Status (field r) — field [13], pre-GS part
    if len(f) > 13:
        status_field = f[13]
        gs_idx = status_field.find(GS)
        status_raw = status_field[:gs_idx] if gs_idx != -1 else status_field
        cam_part   = status_field[gs_idx:] if gs_idx != -1 else ""

        if status_raw:
            msg.fields.append(("─── Last Transaction Status ───", ""))
            serial      = status_raw[0:4]   if len(status_raw) >= 4  else ""
            last_status = status_raw[4:5]   if len(status_raw) >= 5  else ""
            notes_disp  = status_raw[5:25]  if len(status_raw) >= 25 else ""
            coins_disp  = status_raw[25:45] if len(status_raw) >= 45 else ""
            deposit_dir = status_raw[45:46] if len(status_raw) >= 46 else ""

            if serial:
                msg.fields.append(("  Serial Number",     serial))
            if last_status:
                status_label = LAST_STATUS_CODES.get(
                    last_status, f"Unknown ({last_status})")
                msg.fields.append(("  Last Status Issued", f"{last_status} — {status_label}"))
            if notes_disp and notes_disp.strip("0"):
                # 4 cassettes × 5-digit counts (NDC option 76 = 000)
                cassette_counts = [notes_disp[i:i+5] for i in range(0, 20, 5)]
                for ci, cnt in enumerate(cassette_counts):
                    if cnt.strip("0"):
                        msg.fields.append((f"  Cassette {ci+1} Dispensed", str(int(cnt))))
            if deposit_dir:
                DEPOSIT_DIR = {"0": "Not a cash deposit", "1": "Vault direction",
                               "2": "Refund direction"}
                msg.fields.append(("  Cash Deposit Dir",
                                   DEPOSIT_DIR.get(deposit_dir, deposit_dir)))

        # CAM block with current transaction EMV TLV
        if cam_part:
            emv = _decode_cam_block(cam_part)
            msg.emv_tags = emv
            msg.fields.append(("─── EMV (Current Transaction) ───", ""))
            for key in ("Transaction Type", "Amount Authorised",
                        "Transaction Currency Code", "Transaction Date",
                        "Cryptogram Information Data", "Application Identifier"):
                val = emv.get(key)
                if val:
                    msg.fields.append((f"  {key}", val))

    # 5CAM block (field [14]) — previous transaction issuer auth data
    if len(f) > 14:
        cam5 = f[14].strip()
        if cam5.upper().startswith("5CAM"):
            prev_emv = _decode_5cam_block(cam5)
            if prev_emv:
                msg.fields.append(("─── EMV (Previous Txn Issuer Auth) ───", ""))
                for key in ("Application Cryptogram", "Cryptogram Information Data",
                            "Issuer Auth Data"):
                    val = prev_emv.get(key)
                    if val:
                        msg.fields.append((f"  {key}", val))


def _decode_22(f: list, msg: NdcMessage) -> None:
    """Go Next State / Acknowledge  (HOST->ATM)

    Spec field layout:
        [0]  class "22"
        [1]  LUNO
        [2]  reserved (empty)
        [3]  Next State code:
               '9' = Supervisor/Idle ACK
               'B' = Transaction Reply (Ready — txn complete)
               'C' = Specific Command Reject
               'F' = Terminal State / Config sequence
        [4]  State-dependent data:
               For 'B': Transaction serial number + optional CAM\x1d<EMV TLV>
               For 'C': Rejection reason code (e.g. "C01")
               For 'F': Config response payload, variants:
                 "5025MMDDHHMMSS"  -> Terminal date/time response
                 "LA<data>"        -> Language/locale config
                 "HA<data>"        -> Hardware config params
                 "IA<data>\x1dE...\x1dG..."  -> Input config params
                 "JA<data>"        -> Journal config params
        [5]  Additional config field (for 'F' multi-field responses, e.g. 'B11')
        [6]  Further GS-delimited config key=value pairs
    """
    luno       = f[1].strip() if len(f) > 1 else ""
    next_state = f[3].strip() if len(f) > 3 else ""
    field4     = f[4] if len(f) > 4 else ""

    if luno: msg.fields.append(("LUNO", luno))

    NEXT_STATE_LABELS = {
        "9": "Supervisor / Idle (ACK)",
        "B": "Ready — Transaction Complete",
        "C": "Specific Command Reject",
        "F": "Terminal State / Config",
    }
    state_label = NEXT_STATE_LABELS.get(next_state, f"State {next_state}")
    if next_state:
        msg.fields.append(("Next State", f"{next_state}  ({state_label})"))
        msg.summary += f" -> {state_label}"

    if not field4:
        return

    if next_state == "9":
        # Simple ACK — no additional fields expected
        pass

    elif next_state == "B":
        # Transaction serial number + optional CAM EMV block
        seq_num  = field4.split(GS)[0].strip()
        cam_part = field4[len(seq_num):]
        if seq_num:
            msg.fields.append(("Transaction Serial", seq_num))
        if cam_part and GS in cam_part:
            emv = _decode_cam_block(cam_part)
            msg.emv_tags = emv
            crypt = emv.get("Cryptogram Information Data")
            ac    = emv.get("Application Cryptogram")
            if crypt: msg.fields.append(("Cryptogram Type",       crypt))
            if ac:    msg.fields.append(("Application Cryptogram", ac))

    elif next_state == "C":
        # Specific Command Reject — field[4] = rejection reason
        REJECT_REASONS = {
            "C01": "Authentication failure",
            "C02": "Sequence error",
            "C03": "Message too long",
        }
        reason = field4.strip()
        msg.fields.append(("Reject Reason",
                            REJECT_REASONS.get(reason, reason)))

    elif next_state == "F":
        # Terminal State / Config response
        _decode_22_terminal_state(f, field4, msg)


def _decode_22_terminal_state(f: list, field4: str, msg: NdcMessage) -> None:
    """Decode 'F' (Terminal State) sub-variants from the config data fields."""

    CONFIG_IDS = {
        "50": "Terminal Date/Time",
        "LA": "Language / Locale Config",
        "HA": "Hardware Config Parameters",
        "IA": "Input Config Parameters",
        "JA": "Journal Config Parameters",
    }

    # Identify sub-type from first 2 chars of field4
    sub_id    = field4[:2].upper()
    sub_label = CONFIG_IDS.get(sub_id, f"Config ({sub_id})")
    msg.fields.append(("Config Type", sub_label))

    if sub_id == "50":
        # "5025MMDDHHMMSS" — terminal date/time sync
        # field4 = "5025" + "MMDD" + "HHMM" + "SS"
        payload = field4[2:]   # "25MMDDHHMMSS" — "25" is the terminal ID suffix
        if len(payload) >= 12:
            term   = payload[0:2]
            mmdd   = payload[2:6]
            hhmmss = payload[6:12]
            msg.fields.append(("Terminal Suffix", term))
            msg.fields.append(("Date",  f"{mmdd[0:2]}/{mmdd[2:4]}"))
            msg.fields.append(("Time",  f"{hhmmss[0:2]}:{hhmmss[2:4]}:{hhmmss[4:6]}"))

    elif sub_id in ("HA", "LA", "IA", "JA"):
        # Key-value config params — GS-delimited K<n>=<value> pairs
        # field4 example: "HA1715" or "HA0000"
        # field[5] = "B11"
        # field[6] = "CC00\x1dD09\x1dE04\x1dG01\x1dH80\x1dLD7\x1dP01\x1dS02\x1dZ02"
        raw_value = field4[2:].strip()
        if raw_value:
            msg.fields.append((f"  {sub_id} Value", raw_value))

        # Collect remaining FS fields + their GS-delimited sub-params
        for seg in f[5:]:
            seg = seg.strip()
            if not seg: continue
            # GS-delimited key=value pairs
            parts = seg.split(GS)
            for part in parts:
                if len(part) >= 2:
                    key = part[0]
                    val = part[1:]
                    msg.fields.append((f"  Param [{key}]", val))

    else:
        # Generic — show raw value
        if field4[2:].strip():
            msg.fields.append(("Value", field4[2:].strip()))


def _decode_40(f: list, msg: NdcMessage) -> None:
    """Transaction Reply Command  (HOST->ATM)

    Spec: Table 10-15 — Transaction Reply Command Message Format
    Class '4' + response_flag = "40" (response_flag '0' = standard).

    Layout:
        [0]  b+c  "4" + response_flag (usually '0')
        [1]  d    LUNO (optional, 3 or 9 chars)
        [2]  e    TVN — Time Variant Number (3 or 8 chars, optional)
        [3]  f    Next State ID (3 chars, base-10 or base-36)
        [4]  g1-g7  Note counts to dispense (2 or 4 chars each, packed, no FS)
        [5]  g_remaining + serial(4) + func_id(1) + SI(0x0F) + receipt_flag + receipt
        [6]  serial(4) + func_id(1)  (when [5] contains only note counts)
             OR: 5CAM block (issuer auth)
        ...
    For Print Immediate (func='4'):
        [5]  serial(4) + func_id(1) = "XXXX4"
        [6]  journal/status text

    func_id values:
        '1'/'7' = Deposit and Print
        '2'/'8' = Dispense and Print
        '3'/'9' = Display and Print
        '4'     = Print Immediate
    """
    resp_flag  = f[0][1:] if len(f[0]) > 1 else ""
    luno       = f[1].strip() if len(f) > 1 else ""
    tvn        = f[2].strip() if len(f) > 2 else ""
    next_state = f[3].strip() if len(f) > 3 else ""
    note_field = f[4].strip() if len(f) > 4 else ""
    field5     = f[5] if len(f) > 5 else ""

    if luno:       msg.fields.append(("LUNO",           luno))
    if tvn:        msg.fields.append(("TVN",            tvn))
    if next_state: msg.fields.append(("Next State",     next_state))

    # Detect Print Immediate (func='4') — serial+func in field[5], text in field[6]
    if not note_field and field5 and len(field5) >= 5:
        serial  = field5[:4]
        func_id = field5[4:5]
        text    = f[6] if len(f) > 6 else ""
        func_label = TXN_REPLY_FUNCTIONS.get(func_id, f"Function {func_id}")
        msg.fields.append(("Transaction Serial", serial))
        msg.fields.append(("Function",           f"{func_id}  —  {func_label}"))
        msg.summary += f": {func_label}"
        if text:
            msg.receipt_lines = _clean_text(text)
            for line in msg.receipt_lines:
                if line.startswith("***"):
                    msg.fields.append(("Status Event", line.lstrip("* ").strip()))
        return

    # Standard dispense/display reply
    # Note counts in field[4] (2 chars × up to 4 cassettes) and field[5] prefix
    if note_field:
        note_counts = [note_field[i:i+2] for i in range(0, len(note_field), 2)]
        dispensed = [(i+1, int(c)) for i, c in enumerate(note_counts)
                     if c.strip() and c.strip("0")]
        if dispensed:
            msg.fields.append(("─── Notes to Dispense ───", ""))
            for cassette, count in dispensed:
                msg.fields.append((f"  Cassette {cassette}", f"{count} notes"))
        elif all(c == "00" for c in note_counts):
            msg.fields.append(("Notes to Dispense", "None (0 for all cassettes)"))

    # Extract serial + func_id + receipt from field[5]
    si = "\x0f"
    if si in field5:
        pre_si, post_si = field5.split(si, 1)
    else:
        pre_si, post_si = field5, ""

    # Last 5 chars of pre_si = serial(4) + func_id(1)
    if len(pre_si) >= 5:
        serial  = pre_si[-5:-1]
        func_id = pre_si[-1]
        func_label = TXN_REPLY_FUNCTIONS.get(func_id, f"Function {func_id}")
        msg.fields.append(("Transaction Serial", serial))
        msg.fields.append(("Function",           f"{func_id}  —  {func_label}"))
        msg.summary += f": {func_label}"

    # Receipt data from post_si or subsequent fields
    receipt_raw = post_si.lstrip("DF")  # strip receipt type flag byte
    for seg in f[6:]:
        seg_s = seg.strip()
        if not seg_s: continue
        if seg_s.upper().startswith("5CAM"):
            emv = _decode_5cam_block(seg_s)
            msg.emv_tags = emv
            ac = emv.get("Application Cryptogram")
            if ac: msg.fields.append(("Issuer Auth Cryptogram", ac))
        elif "\x0a" in seg or seg_s.startswith(":"):
            receipt_raw += seg_s.lstrip(":01")
            break

    if receipt_raw:
        msg.receipt_lines = _clean_text(receipt_raw)
        _extract_receipt_fields(msg)


def _decode_6_ej(f: list, msg: NdcMessage) -> None:
    """EJ (Electronic Journal) Command  (HOST->ATM)

    Spec: Table 10-38/39 — EJ Options and Timers / Acknowledge EJ Upload Block
    Class '6' = Electronic Journal commands.

    Layout:
        [0]  b    "6" (single char — no subclass)
        [1]       FS reserved (empty)
        [2]       FS reserved (empty)
        [3]  c+d+e+f+g  Command payload — first char is Command Type:
               '3' = EJ Options and Timers:
                     "3" + option_num(2) + option_val(3) [repeated] + option_num + option_val
                     + timer_num(2) + timer_val(3)
                     e.g. "36020061000" = opt60=200, opt61=000 (no timer)
                     e.g. "36020061000\1c60255" = opts + timer60=255
               '1' = Acknowledge EJ Upload Block:
                     "1" + last_char_received(6)
                     e.g. "1046204" = ack char position 46204
               '2' = Acknowledge and Stop EJ:
                     "2" + last_char_received(6)
        [4]  Optional: timer_num(2) + timer_val(3)
    """
    payload = f[3] if len(f) > 3 else ""
    if not payload:
        return

    cmd_type  = payload[0]
    cmd_data  = payload[1:]
    cmd_label = EJ_COMMAND_TYPES.get(cmd_type, f"Command {cmd_type}")

    msg.fields.append(("Command Type", f"{cmd_type}  —  {cmd_label}"))
    msg.summary += f": {cmd_label}"

    if cmd_type == "1" or cmd_type == "2":
        # Acknowledge EJ Upload Block or Acknowledge and Stop EJ
        last_char = cmd_data[:6]
        if last_char:
            try:
                msg.fields.append(("Last Char Received",
                                   f"{int(last_char):,}  (character position in EJ)"))
            except ValueError:
                msg.fields.append(("Last Char Received", last_char))
        if cmd_type == "2":
            msg.fields.append(("Action", "EJ upload will STOP after this block"))

    elif cmd_type == "3":
        # EJ Options and Timers — option pairs (2+3 chars) repeated
        msg.fields.append(("─── EJ Options ───", ""))
        i = 0
        while i + 5 <= len(cmd_data):
            opt_num = cmd_data[i:i+2]
            opt_val = cmd_data[i+2:i+5]
            opt_info = EJ_OPTIONS.get(opt_num, (f"Option {opt_num}", ""))
            opt_name, opt_desc = opt_info
            try:
                display_val = str(int(opt_val))
            except ValueError:
                display_val = opt_val
            msg.fields.append((f"  {opt_name}", f"{display_val}  ({opt_desc})"))
            i += 5

        # Timer fields — either packed in payload or in field[4]
        timer_field = f[4].strip() if len(f) > 4 else cmd_data[i:]
        if timer_field and len(timer_field) >= 5:
            timer_num = timer_field[:2]
            timer_val = timer_field[2:5]
            timer_name = EJ_TIMER_NAMES.get(timer_num, f"Timer {timer_num}")
            try:
                t_display = str(int(timer_val))
            except ValueError:
                t_display = timer_val
            msg.fields.append(("─── EJ Timer ───", ""))
            msg.fields.append((f"  {timer_name}", t_display))


def _decode_10(f: list, msg: NdcMessage) -> None:
    luno   = f[1].strip() if len(f) > 1 else ""
    seq    = f[2].strip() if len(f) > 2 else ""
    reason = f[3].strip() if len(f) > 3 else ""

    if luno:   msg.fields.append(("Terminal LUNO", luno))
    if seq:    msg.fields.append(("Sequence",      seq))

    reason_label = GOS_REASON_CODES.get(reason, f"Reason code {reason}")
    if reason:
        msg.fields.append(("Reason Code", f"{reason}  —  {reason_label}"))
        msg.summary += f": {reason_label}"


def _decode_12(f: list, msg: NdcMessage) -> None:
    """Status / Ready  (ATM->HOST)

    Class "12" = class 1 (Unsolicited) + subclass 2 (Status).
    Layout:
        [0]  "12"
        [1]  LUNO
        [2]  reserved (empty)
        [3]  Status Information -- first char is Subtype key:
               B = Config/firmware version ("B" + 4-char value)
               P = Peripheral/device status ("P" + 19-char bitmask)
               E = Cassette/supply status ("E" + 9-char supply string)
        [4+] Additional FS fields for subtype E: note counts
    """
    luno        = f[1].strip() if len(f) > 1 else ""
    subtype_raw = f[3] if len(f) > 3 else ""

    if luno: msg.fields.append(("LUNO", luno))
    if not subtype_raw: return

    key   = subtype_raw[0].upper()
    data  = subtype_raw[1:]
    label = STATUS_READY_SUBTYPES.get(key, "Subtype " + key)

    msg.fields.append(("Subtype", key + "  --  " + label))
    msg.summary += " (" + label + ")"

    if key == "B":
        if data.strip():
            msg.fields.append(("Config Version", data.strip()))

    elif key == "P":
        # Device position map (NDC spec, 15 devices)
        P_DEVICE_MAP = {
            0: "Card Reader",    1: "Cash Handler",      2: "Receipt Printer",
            3: "Journal Printer",4: "Night Safe",         5: "Encryptor",
            6: "Camera",         7: "Supervisor",         8: "Door",
            9: "Fascia",        10: "Statement Printer", 11: "Passbook Printer",
           12: "Barcode Reader", 13: "Cheque Processing", 14: "Coin Dispenser",
        }
        # Non-supply devices where code '1' means 'Not Fitted / Inactive'
        # rather than 'Needs Replenishment' (which only applies to cash/paper devices)
        NON_SUPPLY_DEVICES = {
            7: "Supervisor", 8: "Door", 9: "Fascia",
        }
        msg.fields.append(("--- Peripheral Status ---", ""))
        ok_devices, fault_devices = [], []
        for i, ch in enumerate(data):
            device = P_DEVICE_MAP.get(i)
            if device is None:
                break
            ch_upper = ch.upper()
            if ch_upper == "0":
                ok_devices.append(device)
            elif ch_upper == "1" and i in NON_SUPPLY_DEVICES:
                # For non-supply devices code 1 = not fitted / inactive, treat as OK
                ok_devices.append(device + " (not fitted)")
            else:
                state = DEVICE_STATE_CODES.get(ch_upper, "Unknown (" + ch + ")")
                fault_devices.append((device, state))
        for device, state in fault_devices:
            msg.fields.append(("  ! " + device, state))
        if ok_devices:
            msg.fields.append(("  OK devices", ", ".join(ok_devices)))

    elif key == "E":
        msg.fields.append(("--- Supply Status ---", ""))
        ok_supplies, fault_supplies = [], []
        for i, ch in enumerate(data):
            if i >= len(SUPPLY_DEVICE_LABELS): break
            lbl_s  = SUPPLY_DEVICE_LABELS[i]
            status = SUPPLY_STATUS_CODES.get(ch, "Unknown (" + ch + ")")
            if status == "OK": ok_supplies.append(lbl_s)
            else:              fault_supplies.append((lbl_s, status))
        for lbl_s, st in fault_supplies:
            msg.fields.append(("  ! " + lbl_s, st))
        if ok_supplies:
            msg.fields.append(("  OK supplies", ", ".join(ok_supplies)))
        cassette_labels = ["Cassette 1 Count", "Cassette 2 Count",
                           "Cassette 3 Count", "Cassette 4 Count",
                           "Reject Bin Count"]
        cas_idx = 0
        for seg in f[4:]:
            seg = seg.strip()
            if not seg: continue
            chunks = [seg[i:i+5] for i in range(0, len(seg), 5)]
            for chunk in chunks:
                if chunk and cas_idx < len(cassette_labels):
                    try:    msg.fields.append((cassette_labels[cas_idx], str(int(chunk))))
                    except: msg.fields.append((cassette_labels[cas_idx], chunk))
                    cas_idx += 1


def _decode_30(f: list, msg: NdcMessage) -> None:
    luno     = f[1].strip() if len(f) > 1 else ""
    seq      = f[2].strip() if len(f) > 2 else ""
    func_raw = f[3].strip() if len(f) > 3 else ""

    if luno: msg.fields.append(("Terminal LUNO", luno))
    if seq:  msg.fields.append(("Sequence",      seq))

    func_label = GIS_FUNCTION_CODES.get(func_raw.upper(), f"Function {func_raw}")
    if func_raw:
        msg.fields.append(("Function Code", f"{func_raw}  —  {func_label}"))
        msg.summary += f": {func_label}"

    data_fields = f[4:]
    func_upper  = func_raw.upper()

    if func_upper == "15":
        # Cassette / denomination config
        combined = FS.join(data_fields)
        for part in combined.split(FS):
            part = part.strip()
            if re.match(r"^\d{3}", part):
                count_s = part[:3]; rest = part[3:].strip()
                if rest:
                    msg.fields.append((f"Cassette ({count_s})", rest))

    elif func_upper in ("11", "12", "1C", "1E"):
        # Screen/state table — show first N screen IDs + readable text
        combined = FS.join(data_fields)
        found = 0
        for part in combined.split(FS):
            part = part.strip()
            if not part: continue
            id_m = re.match(r"^([0-9A-Fa-f]{3})", part)
            if id_m:
                scr_id = id_m.group(1)
                clean  = _clean_text(part[3:])
                readable = " | ".join(clean[:4]) if clean else "(control data)"
                msg.fields.append((f"Screen {scr_id}", readable[:100]))
                found += 1
                if found >= 10:
                    remaining = combined.count(FS) - found
                    if remaining > 0:
                        msg.fields.append(("…", f"({remaining} more screens)"))
                    break

    else:
        for i, seg in enumerate(data_fields[:5]):
            if seg.strip():
                msg.fields.append((f"Data [{i}]", seg.strip()[:120]))
        if len(data_fields) > 5:
            msg.fields.append(("…", f"({len(data_fields)-5} more fields)"))


def _decode_31_fit(f: list, msg: NdcMessage) -> None:
    """FIT Data Load  (HOST->ATM)

    Spec: Table 10-7 — FIT Data Load
    Downloads Financial Institution Tables (FITs) to the terminal.
    Up to 1000 FITs can be stored; each FIT controls PIN encryption,
    PIN verification, and indirect next-state processing for a card range.

    Layout:
        [0]  b+c   "3" + response_flag (optional, ignored by terminal)
        [1]  d     LUNO (3 chars, optional, ignored by terminal)
        [2]  e     Message Sequence Number (3 chars, optional, ignored)
        [3]  f+g   Sub-class "1" + Identifier "5" packed = "15"
        [4]  h     FIT Number (3 chars, 000–999, defines search order)
        [5]  i     FIT Data (var, up to 41 three-char decimal entries, 000–255)
        [6]  j     FIT Number (3 chars) — repeated per Table Note 13
        [7]  k     FIT Data (var) — repeated per Table Note 13
        ...        Fields j+k repeat until protocol length limit
        [-2] FS    Field Separator (only if MAC present — Table Note 14)
        [-1] l     MAC Data (8 chars, 0-9 A-F, only if auth flags set)
    """
    resp_flag = f[0][1:] if len(f[0]) > 1 else ""
    luno      = f[1].strip() if len(f) > 1 else ""
    seq       = f[2].strip() if len(f) > 2 else ""
    subclass  = f[3].strip() if len(f) > 3 else ""

    if luno: msg.fields.append(("LUNO",              luno))
    if seq:  msg.fields.append(("Sequence Number",   seq))

    # Verify subclass+identifier = "15"
    if subclass != "15":
        msg.fields.append(("Sub-class/Identifier", subclass))
    else:
        msg.fields.append(("Type", "Customisation Data — FIT Data"))

    # FIT entries: field[4]=FIT_num, field[5]=FIT_data, repeated
    # Last field may be MAC (8 hex chars) if message auth is enabled
    fit_fields = f[4:]

    # Detect trailing MAC: last field = 8 hex chars with no FIT num before it
    mac = ""
    if fit_fields and re.match(r"^[0-9A-Fa-f]{8}$", fit_fields[-1].strip()):
        mac = fit_fields[-1].strip()
        fit_fields = fit_fields[:-1]

    # Parse FIT num+data pairs
    fit_count = 0
    i = 0
    while i < len(fit_fields) - 1:
        fit_num  = fit_fields[i].strip()
        fit_data = fit_fields[i+1].strip()
        i += 2

        if not fit_num: continue

        # FIT data = series of 3-char decimal entries (000-255)
        # Each entry is a control word for PIN/next-state processing
        entries = [fit_data[j:j+3] for j in range(0, len(fit_data), 3)
                   if fit_data[j:j+3]]
        entry_count = len(entries)
        entry_preview = ", ".join(entries[:6])
        if entry_count > 6:
            entry_preview += f"… (+{entry_count-6} more)"

        msg.fields.append((f"FIT {fit_num}",
                           f"{entry_count} entries: {entry_preview}"))
        fit_count += 1

    msg.fields.append(("FIT Tables Loaded", str(fit_count)))
    if mac:
        msg.fields.append(("MAC Data", mac))


def _decode_80(f: list, msg: NdcMessage) -> None:
    luno    = f[1].strip() if len(f) > 1 else ""
    subtype = f[2].strip() if len(f) > 2 else ""
    payload = f[3].strip() if len(f) > 3 else ""

    if luno: msg.fields.append(("Terminal LUNO", luno))

    LABELS = {"1": "Terminal Key / Currency Config", "2": "Transaction TLV Limits",
              "4": "Terminal Data", "5": "EMV AID Table"}
    label = LABELS.get(subtype, f"Subtype {subtype}")
    msg.fields.append(("Config Type", f"{subtype}  —  {label}"))
    msg.summary += f": {label}"

    if not payload: return

    if subtype == "1":
        emv = _decode_emv_tlv(payload)
        for k, v in emv.items():
            msg.fields.append((k, v))
    elif subtype == "2":
        for idx, m in enumerate(re.finditer(r"77([0-9A-Fa-f]{2})(.+?)(?=77[0-9A-Fa-f]{2}|$)", payload)):
            emv = _decode_emv_tlv(m.group(2))
            txn_type = emv.get("Transaction Type", "?")
            msg.fields.append((f"Profile [{idx}] txn type", txn_type))
            if idx >= 7: break
    elif subtype == "4":
        emv = _decode_emv_tlv(payload)
        for k, v in list(emv.items())[:6]:
            msg.fields.append((k, v))
    elif subtype >= "5":
        # AID table entry
        try:
            seq_idx = int(payload[0:2], 16)
            aid_len = int(payload[2:4], 16)
            aid_hex = payload[4:4+aid_len*2]
            rest    = payload[4+aid_len*2:]
            lbl_m   = re.search(r"([A-Z][A-Z0-9 _\-]{2,30})(?:CAM|$)", rest)
            lbl     = lbl_m.group(1).strip() if lbl_m else ""
            msg.fields.append((f"AID [{seq_idx:02d}]",
                                f"A0{aid_hex[2:]} — {lbl}" if lbl else f"A0{aid_hex[2:]}"))
        except (ValueError, IndexError):
            msg.fields.append(("AID Data (raw)", payload[:60]))


def _decode_23(f: list, msg: NdcMessage) -> None:
    """Encryptor Initialisation Data  (ATM->HOST)

    Spec Table 9-37. Class 23 = class 2 (Solicited) + subclass 3 (Encryptor Init).
    Returned in response to an Extended Encryption Key Change command.

    Layout:
        [0]  23
        [1]  LUNO
        [2]  FS reserved (empty)
        [3]  e  Information Identifier (1 char) -- see ENCRYPTOR_INFO_IDS
        [4]  f  Encryptor Information (variable, depends on [3])
    """
    luno    = f[1].strip() if len(f) > 1 else ""
    info_id = f[3].strip() if len(f) > 3 else ""
    data    = f[4].strip() if len(f) > 4 else ""

    if luno: msg.fields.append(("LUNO", luno))

    id_label = ENCRYPTOR_INFO_IDS.get(info_id.upper(),
                                       "Unknown Identifier (" + info_id + ")")
    if info_id:
        msg.fields.append(("Information Identifier",
                           info_id + "  --  " + id_label))
        msg.summary += ": " + id_label

    if not data:
        return

    if info_id == "3":
        msg.fields.append(("New KVV", data))
        length_label = ("Single-length (24-bit)" if len(data) == 6
                        else "Double-length (288-bit)" if len(data) == 72
                        else str(len(data)) + " chars")
        msg.fields.append(("KVV Length", length_label))
    elif info_id == "4":
        for i, name in enumerate(["Master Key KVV", "Communications Key KVV",
                                   "MAC Key KVV",    "B Key KVV"]):
            kvv = data[i*6:(i+1)*6] if len(data) >= (i+1)*6 else ""
            if kvv:
                status = ("Not loaded" if kvv == "000000"
                          else "No KVV available" if kvv.strip() == ""
                          else kvv)
                msg.fields.append(("  " + name, status))
    elif info_id == "6":
        mode = data[0] if data else ""
        msg.fields.append(("Key Entry Mode",
                           mode + "  --  " + KEY_ENTRY_MODES.get(mode, "Unknown")))
    elif info_id == "1":
        msg.fields.append(("EPP Serial Number", data[:8]))
        if len(data) > 8:
            msg.fields.append(("Signature",
                               data[8:16] + "... (" + str(len(data)-8) + " chars)"))
    elif info_id == "2":
        msg.fields.append(("Public Key (base-94)",
                           data[:16] + "... (" + str(len(data)) + " chars total)"))
    elif info_id == "B":
        msg.fields.append(("Capabilities / State", data[:80]))
    else:
        msg.fields.append(("Data", data[:80]))


def _decode_61(f: list, msg: NdcMessage) -> None:
    """Solicited Status Message  (ATM->HOST)

    Spec: Solicited Status Message Format (Table 9-7)
        [0]  b+c  Message class+subclass = "61" (DN extension of spec "22")
        [1]  d    LUNO — empty in broadcast status messages
        [2]       FS reserved
        [3]       FS reserved
        [4]       Packed status blob (no internal FS separators):
                    [0:8]   Unknown header / terminal ID prefix (fixed "02200025")
                    [8:12]  Date MMDD
                    [12:16] Time HHMM
                    [16:30] Hardware Device Status Block — 14 chars, one per device
                            Each char: '0'=OK, '1'=Needs Replenishment,
                            '2'=Inoperative, '3'=Needs Maintenance,
                            '4'=In Supervisor Mode, '5'=Busy, '6'=Offline,
                            '7'=Error, '8'=Threshold, '9'=Needs Maintenance,
                            'A'=Serious Fault, 'B'=Hardware Error,
                            'C'=Config Error, 'D'=Disabled
                    [30]    Status Descriptor:
                            '8'=Device Fault, '9'=Ready, 'A'=Command Reject,
                            'B'=Ready (Txn Complete), 'C'=Specific Cmd Reject,
                            'F'=Terminal State
                            DN extensions: '0','1','2' = Ready variants
                    [31:]   Status Information + human-readable timestamp text
                            + *** status event lines
    """
    luno = f[1].strip() if len(f) > 1 else ""
    blob = f[4] if len(f) > 4 else ""

    if luno:
        msg.fields.append(("LUNO", luno))

    if not blob:
        return

    # Fixed header (positions 0-7) — DN terminal ID prefix, not decoded further
    hdr  = blob[0:8]

    # Date + Time (positions 8-15)
    mmdd = blob[8:12]
    hhmm = blob[12:16]
    if len(mmdd) == 4 and mmdd.isdigit():
        msg.fields.append(("Date",  f"{mmdd[0:2]}/{mmdd[2:4]}"))
    if len(hhmm) == 4 and hhmm.isdigit():
        msg.fields.append(("Time",  f"{hhmm[0:2]}:{hhmm[2:4]}"))

    # Status Descriptor (position 30)
    descriptor = blob[30] if len(blob) > 30 else ""
    DESCRIPTOR_LABELS = {
        "8": "Device Fault",
        "9": "Ready",
        "A": "Command Reject",
        "B": "Ready (Transaction Reply Completed)",
        "C": "Specific Command Reject",
        "F": "Terminal State",
        "0": "Ready",
        "1": "Ready",
        "2": "Ready",
    }
    if descriptor:
        label = DESCRIPTOR_LABELS.get(descriptor, f"Unknown ({descriptor})")
        msg.fields.append(("Status Descriptor", f"{descriptor} — {label}"))
        msg.summary += f": {label}"

    # Hardware Device Status Block (positions 16-29, exactly 14 devices)
    hw_block = blob[16:30] if len(blob) >= 30 else blob[16:]
    if hw_block:
        msg.fields.append(("─── Device States ───", ""))
        ok_devices    = []
        fault_devices = []
        for i in range(min(len(hw_block), len(DEVICE_STATUS_MAP))):
            ch     = hw_block[i].upper()
            device = DEVICE_STATUS_MAP[i]
            state  = DEVICE_STATE_CODES.get(ch, f"Unknown (0x{ord(hw_block[i]):02X})")
            if state == "OK":
                ok_devices.append(device)
            else:
                fault_devices.append((device, state))

        for device, state in fault_devices:
            msg.fields.append((f"  ⚠ {device}", state))
        if ok_devices:
            msg.fields.append(("  ✓ OK devices",
                               ", ".join(ok_devices)))

    # Status Information + event lines (position 31+)
    info = blob[31:] if len(blob) > 31 else ""
    if info:
        # Extract *** status event lines
        for line in re.split(r"[\r\n]+", info):
            line = line.strip()
            if line.startswith("***"):
                msg.fields.append(("Status Event",
                                   line.lstrip("* ").strip()))
        # Precise timestamp from text portion
        ts_m = re.search(r"(\d{1,2}:\d{2}:\d{2}\s+\d{2}/\d{2}/\d{4})", info)
        if ts_m:
            msg.fields.append(("Timestamp", ts_m.group(1).strip()))


def _extract_receipt_fields(msg: NdcMessage) -> None:
    text = "\n".join(msg.receipt_lines)
    for pattern, label in [
        (r"TRANSACTION[:\s]+([^\n\\]{3,40})",    "Transaction"),
        (r"PAN[:\s]+([^\n\\]{10,30})",           "PAN (masked)"),
        (r"TRN NUMBER[:\s]+(\d+)",               "Transaction Number"),
        (r"STAN[:\s]+(\d+)",                     "STAN"),
        (r"RESPONSE CODE[:\s]+(\S+)",            "Response Code"),
        (r"INTERNAL[:\s]+(\d+)",                 "Internal Code"),
        (r"REQUESTED AMOUNT[:\s]+([\d.,]+)",      "Requested Amount"),
        (r"DEBIT AMOUNT[:\s]+([\d.,]+)",          "Debit Amount"),
        (r"ISSUER FEE[:\s]+([\d.,]+)",            "Issuer Fee"),
        (r"A/C BALANCE[:\s]+([\d.,]+)",           "Account Balance"),
        (r"CASH AVAILABLE[:\s]+([\d.,]+)",        "Cash Available"),
        (r"TERMINAL ID[:\s]+(\S+)",               "Terminal ID"),
        (r"ACCOUNT[:\s]+(\*[\*\d]+\d{4})",       "Account (masked)"),
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip().split("\n")[0].split("\\")[0].strip()
            if val and val not in ("N/A", "0", ""):
                msg.fields.append((label, val))


# ─────────────────────────────────────────────────────────────────────────────
# Main decode entry point
# ─────────────────────────────────────────────────────────────────────────────

def decode_message(raw: str) -> NdcMessage:
    unescaped = _unescape(raw.strip())
    fields    = _split_fs(unescaped)
    cls       = fields[0].strip().upper() if fields else "??"
    class_info = MESSAGE_CLASS_MAP.get(cls, (f"Unknown Class ({cls})", "UNKNOWN"))
    summary, direction = class_info

    msg = NdcMessage(raw=raw.strip(), message_class=cls,
                     direction=direction, summary=summary)
    try:
        if   cls == "11":                       _decode_11(fields, msg)
        elif cls == "22":                       _decode_22(fields, msg)
        elif cls in ("40", "41"):               _decode_40(fields, msg)
        elif cls == "10":                       _decode_10(fields, msg)
        elif cls == "12":                       _decode_12(fields, msg)
        elif cls == "30":                       _decode_30(fields, msg)
        elif cls == "31":                       _decode_31_fit(fields, msg)
        elif cls == "80":                       _decode_80(fields, msg)
        elif cls == "23":                       _decode_23(fields, msg)
        elif cls == "6":                        _decode_6_ej(fields, msg)
        elif cls in ("61","62","70","71"):       _decode_61(fields, msg)
        else:
            for i, seg in enumerate(fields[1:], 1):
                if seg.strip(): msg.fields.append((f"Field[{i}]", seg.strip()[:120]))
    except Exception as exc:
        msg.errors.append(f"Parse error: {exc}")

    return msg


_NDC_LINE_RE = re.compile(r"^[0-9A-Fa-f]{1,2}(?:\\1c|\x1c)")


def decode_log_block(raw_block: str) -> list:
    return [decode_message(l.strip())
            for l in raw_block.splitlines()
            if l.strip() and _NDC_LINE_RE.match(l.strip())]


def extract_messages_from_jrn(jrn_text: str) -> list:
    """Extract NDC messages from raw .jrn file content (41004/41005 log lines).

    Only handles Format A — real raw NDC messages from 41004/41005 log lines:
        10:47:56  41004 <CCProtFW1> Sent raw message     : 11\1c003\1c\1c...
        10:48:01  41005 <CCProtFW1> Received raw message : 40\1c003\1c...
    """
    messages = []
    seen_raws = set()

    for line in jrn_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if ("41004" in line or "41005" in line) and "raw message" in line.lower():
            # Find "raw message" then the colon after it — avoids rfind picking
            # up ": " inside rejoined receipt/screen data on the same line
            _rm_idx = line.lower().find("raw message")
            _colon_idx = line.find(": ", _rm_idx)
            if _colon_idx == -1:
                continue
            raw = line[_colon_idx+2:].strip().rstrip("\r\n")
            if not (raw and _NDC_LINE_RE.match(raw)):
                continue
            if raw in seen_raws:
                continue
            seen_raws.add(raw)
            m = decode_message(raw)
            if "41004" in line:
                m.direction = "ATM->HOST"
            elif "41005" in line:
                m.direction = "HOST->ATM"
            # Extract timestamp from the log line (HH:MM:SS)
            _ts_m = re.match(r"(\d{2}:\d{2}:\d{2})", line.strip())
            if _ts_m:
                m.timestamp = _ts_m.group(1)
            messages.append(m)

    return messages


def parse_ndc_log_file(file_path) -> list:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Not found: {file_path}")
    return extract_messages_from_jrn(
        path.read_text(encoding="latin-1", errors="replace"))